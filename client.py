#!/usr/bin/env python3
"""
gRPC Media Streaming Client
Tests both audio and video streaming with performance metrics.
"""

import asyncio
import statistics
import time
from typing import Optional

import grpc
import aiofiles

import media_pb2
import media_pb2_grpc


class MediaStreamClient:
    """Client for testing media streaming performance."""
    
    def __init__(self, server_address: str = "localhost:50051"):
        self.server_address = server_address
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[media_pb2_grpc.MediaStreamerStub] = None
    
    async def connect(self):
        """Establish connection to gRPC server."""
        self.channel = grpc.aio.insecure_channel(
            self.server_address,
            options=[
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ]
        )
        self.stub = media_pb2_grpc.MediaStreamerStub(self.channel)
        
        try:
            await grpc.aio.channel_ready(self.channel)
            print(f"✓ Connected to {self.server_address}")
        except grpc.aio.AioRpcError as e:
            print(f"✗ Connection failed: {e}")
            raise
    
    async def disconnect(self):
        """Close the connection."""
        if self.channel:
            await self.channel.close()
            print("✓ Disconnected")
    
    async def stream_audio(
        self,
        video_id: str,
        save_to_file: Optional[str] = None,
        verbose: bool = True
    ) -> dict:
        """Stream audio and return performance metrics."""
        return await self._stream_media(video_id, "audio", save_to_file, verbose)
    
    async def stream_video(
        self,
        video_id: str,
        save_to_file: Optional[str] = None,
        verbose: bool = True
    ) -> dict:
        """Stream video and return performance metrics."""
        return await self._stream_media(video_id, "video", save_to_file, verbose)
    
    async def _stream_media(
        self,
        video_id: str,
        media_type: str,
        save_to_file: Optional[str] = None,
        verbose: bool = True
    ) -> dict:
        """Stream media and measure performance."""
        request = media_pb2.StreamRequest(video_id=video_id)
        
        chunks_received = 0
        total_bytes = 0
        first_chunk_time: Optional[float] = None
        from_cache = False
        file_size = 0
        
        start_time = time.perf_counter()
        
        try:
            # Open file for writing if specified
            file_handle = None
            if save_to_file:
                file_handle = await aiofiles.open(save_to_file, 'wb')
            
            # Choose the appropriate streaming method
            if media_type == "audio":
                stream = self.stub.StreamAudio(request)
            else:
                stream = self.stub.StreamVideo(request)
            
            # Stream chunks
            async for response in stream:
                now = time.perf_counter()
                
                if first_chunk_time is None:
                    first_chunk_time = now - start_time
                    from_cache = response.from_cache
                    file_size = response.file_size
                
                chunks_received += 1
                total_bytes += len(response.chunk)
                
                if file_handle:
                    await file_handle.write(response.chunk)
                
                if verbose and chunks_received % 50 == 0:
                    elapsed = now - start_time
                    speed_mbps = (total_bytes * 8 / 1_000_000) / elapsed if elapsed > 0 else 0
                    source = "CACHE" if response.from_cache else "DOWNLOAD"
                    print(f"  → [{source}] Chunk {chunks_received}: {total_bytes/1024/1024:.2f} MB @ {speed_mbps:.2f} Mbps")
            
            total_time = time.perf_counter() - start_time
            
            if file_handle:
                await file_handle.close()
            
            # Calculate metrics
            metrics = {
                'video_id': video_id,
                'media_type': media_type,
                'chunks_received': chunks_received,
                'total_bytes': total_bytes,
                'total_mb': total_bytes / (1024 * 1024),
                'first_chunk_latency_ms': (first_chunk_time or 0) * 1000,
                'total_time_ms': total_time * 1000,
                'throughput_mbps': (total_bytes * 8 / 1_000_000) / total_time if total_time > 0 else 0,
                'from_cache': from_cache,
                'file_size': file_size,
            }
            
            return metrics
            
        except grpc.aio.AioRpcError as e:
            print(f"✗ RPC Error: {e.code()} - {e.details()}")
            raise
    
    async def benchmark_cached_performance(
        self,
        video_id: str,
        media_type: str = "audio",
        iterations: int = 3
    ) -> None:
        """Benchmark cache performance with multiple iterations."""
        print(f"\n{'='*70}")
        print(f"BENCHMARK: {media_type.upper()} - {video_id} ({iterations} iterations)")
        print(f"{'='*70}")
        
        cold_latencies = []
        cached_latencies = []
        
        for i in range(iterations):
            print(f"\n--- Iteration {i+1}/{iterations} ---")
            
            # First request (cold or cached from previous run)
            print(f"Request 1 (Cold/Cached):")
            metrics1 = await self._stream_media(video_id, media_type, verbose=False)
            cold_latencies.append(metrics1['first_chunk_latency_ms'])
            source1 = "CACHE" if metrics1['from_cache'] else "COLD"
            print(f"  Source: {source1}")
            print(f"  First chunk latency: {metrics1['first_chunk_latency_ms']:.2f} ms")
            print(f"  Total data: {metrics1['total_mb']:.2f} MB")
            
            # Small delay between requests
            await asyncio.sleep(0.2)
            
            # Second request (should definitely be cached now)
            print(f"Request 2 (Cache test):")
            metrics2 = await self._stream_media(video_id, media_type, verbose=False)
            cached_latencies.append(metrics2['first_chunk_latency_ms'])
            source2 = "CACHE" if metrics2['from_cache'] else "COLD"
            print(f"  Source: {source2}")
            print(f"  First chunk latency: {metrics2['first_chunk_latency_ms']:.2f} ms")
            
            if not metrics1['from_cache'] and metrics2['from_cache']:
                speedup = metrics1['first_chunk_latency_ms'] / max(metrics2['first_chunk_latency_ms'], 0.1)
                print(f"  ⚡ Cache speedup: {speedup:.1f}x faster")
        
        # Summary
        print(f"\n{'='*70}")
        print("BENCHMARK SUMMARY")
        print(f"{'='*70}")
        if cold_latencies and cached_latencies:
            print(f"Avg first-chunk latency (cold):   {statistics.mean(cold_latencies):.2f} ms")
            print(f"Avg first-chunk latency (cached): {statistics.mean(cached_latencies):.2f} ms")
            if statistics.mean(cached_latencies) > 0:
                avg_speedup = statistics.mean(cold_latencies) / statistics.mean(cached_latencies)
                print(f"Average cache speedup: {avg_speedup:.1f}x")


async def test_both_media_types(client: MediaStreamClient, video_id: str):
    """Test both audio and video streaming for a video."""
    print(f"\n{'='*70}")
    print(f"TESTING BOTH MEDIA TYPES: {video_id}")
    print(f"{'='*70}")
    
    # Test Audio
    print(f"\n--- AUDIO STREAM ---")
    audio_file = f"/tmp/{video_id}_audio.mp3"
    audio_metrics = await client.stream_audio(video_id, save_to_file=audio_file)
    
    print(f"\nAudio Results:")
    print(f"  Source: {'CACHE' if audio_metrics['from_cache'] else 'DOWNLOAD'}")
    print(f"  First chunk latency: {audio_metrics['first_chunk_latency_ms']:.2f} ms")
    print(f"  Total time: {audio_metrics['total_time_ms']:.2f} ms")
    print(f"  Throughput: {audio_metrics['throughput_mbps']:.2f} Mbps")
    print(f"  Data: {audio_metrics['total_mb']:.2f} MB")
    print(f"  Saved to: {audio_file}")
    
    await asyncio.sleep(0.5)
    
    # Test Video
    print(f"\n--- VIDEO STREAM ---")
    video_file = f"/tmp/{video_id}_video.mp4"
    video_metrics = await client.stream_video(video_id, save_to_file=video_file)
    
    print(f"\nVideo Results:")
    print(f"  Source: {'CACHE' if video_metrics['from_cache'] else 'DOWNLOAD'}")
    print(f"  First chunk latency: {video_metrics['first_chunk_latency_ms']:.2f} ms")
    print(f"  Total time: {video_metrics['total_time_ms']:.2f} ms")
    print(f"  Throughput: {video_metrics['throughput_mbps']:.2f} Mbps")
    print(f"  Data: {video_metrics['total_mb']:.2f} MB")
    print(f"  Saved to: {video_file}")


async def main():
    """Main test function."""
    import sys
    
    # Default test video (copyright-free)
    video_id = sys.argv[1] if len(sys.argv) > 1 else "dQw4w9WgXcQ"
    server_address = sys.argv[2] if len(sys.argv) > 2 else "localhost:50051"
    
    client = MediaStreamClient(server_address)
    
    try:
        await client.connect()
        
        print(f"\n{'='*70}")
        print(f"MEDIA STREAMING TEST CLIENT")
        print(f"{'='*70}")
        print(f"Server: {server_address}")
        print(f"Test Video ID: {video_id}")
        
        # Test both media types
        await test_both_media_types(client, video_id)
        
        # Benchmark audio caching
        await client.benchmark_cached_performance(video_id, "audio", iterations=2)
        
        # Benchmark video caching
        await client.benchmark_cached_performance(video_id, "video", iterations=2)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        raise
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
