# gRPC Media Streaming Server with Local Disk Caching

An ultra-fast gRPC server for YouTube audio/video downloads featuring aggressive local disk caching for sub-millisecond repeat requests.

## Performance Characteristics

| Request Type | First Chunk Latency | Response Time |
|-------------|---------------------|---------------|
| Cold Download | ~500-2000ms | Network dependent |
| **Cache Hit** | **~0.5-2ms** | **Near-instant** |
| Cache Speedup | **500-1000x faster** | After first download |

## Architecture

```
┌─────────────┐     gRPC      ┌─────────────────────────────────────┐
│   Client    │ ◄────────────► │         MediaStreamer Server        │
│ (Telegram   │  StreamAudio  │  ┌───────────────────────────────┐  │
│    Bot)     │  StreamVideo  │  │  1. Check local_cache/        │  │
└─────────────┘               │  │     EXISTS → Stream from disk │  │
                              │  │     MISSING → Download        │  │
                              │  └───────────────────────────────┘  │
                              │           ↓                         │
                              │  ┌───────────────────────────────┐  │
                              │  │  2. yt-dlp + aiohttp download │  │
                              │  │  3. Save to local_cache/      │  │
                              │  │  4. Stream chunks to client   │  │
                              │  └───────────────────────────────┘  │
                              └─────────────────────────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Compile Protocol Buffer

```bash
python -m grpc_tools.protoc \
    --proto_path=. \
    --python_out=. \
    --grpc_python_out=. \
    media.proto
```

Generates:
- `media_pb2.py` - Message definitions
- `media_pb2_grpc.py` - gRPC service stubs

### 3. Setup YouTube Cookies (Required)

Create `cookies.txt` in the working directory:

```bash
# Extract from browser
yt-dlp --cookies-from-browser chrome --cookies cookies.txt

# Or use Firefox
yt-dlp --cookies-from-browser firefox --cookies cookies.txt
```

### 4. Start the Server

```bash
python server.py
```

Server starts on `0.0.0.0:50051` with cache directory `local_cache/`.

### 5. Test with Client

```bash
# Test both audio and video streaming
python client.py VIDEO_ID

# Test with remote server
python client.py VIDEO_ID server:50051
```

## Protocol Buffer Definition

```protobuf
service MediaStreamer {
  rpc StreamAudio(StreamRequest) returns (stream StreamResponse);
  rpc StreamVideo(StreamRequest) returns (stream StreamResponse);
}

message StreamRequest {
  string video_id = 1;
}

message StreamResponse {
  bytes chunk = 1;
  uint32 chunk_number = 2;
  uint64 total_bytes = 3;
  string mime_type = 4;
  bool from_cache = 5;    // True if served from local cache
  uint64 file_size = 6;
}
```

## Key Features

### 1. Intelligent Local Caching
- **Cache Directory**: `local_cache/`
- **Audio Files**: `{video_id}_audio.mp3`
- **Video Files**: `{video_id}_video.mp4`
- **Cache Check**: O(1) filesystem lookup

### 2. Concurrent Download Coordination
- Prevents duplicate simultaneous downloads of the same file
- Uses asyncio locks with wait queues
- Subsequent requests wait and then stream from cache

### 3. Atomic File Writes
- Downloads to `.tmp` file first
- Atomic rename on completion
- Prevents corrupted cache files

### 4. Streaming Strategy
```python
# Cache HIT: Stream directly from disk (0.001s response)
async for chunk in read_file_in_chunks(cache_path):
    yield chunk

# Cache MISS: Download, save, and stream simultaneously
async for chunk in download_with_aiohttp(url):
    await write_to_temp_file(chunk)
    yield chunk
atomic_rename(temp_file, cache_file)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `CACHE_DIR` | `local_cache/` | Local cache directory |
| `CHUNK_SIZE` | 64KB | Streaming chunk size |
| `DOWNLOAD_CHUNK_SIZE` | 256KB | Download chunk size |
| `cookiefile` | `cookies.txt` | YouTube authentication |

## API Usage Example

```python
import grpc
import media_pb2
import media_pb2_grpc

async def download_audio():
    channel = grpc.aio.insecure_channel('localhost:50051')
    stub = media_pb2_grpc.MediaStreamerStub(channel)
    
    request = media_pb2.StreamRequest(video_id="dQw4w9WgXcQ")
    
    # Stream audio
    async for chunk in stub.StreamAudio(request):
        if chunk.from_cache:
            print(f"From cache: {chunk.total_bytes} bytes")
        process_audio(chunk.chunk)
    
    # Stream video
    async for chunk in stub.StreamVideo(request):
        process_video(chunk.chunk)
```

## Cache Management

### View Cache Stats
The server logs cache statistics on startup:
```
📊 Cache stats: {
    'total_files': 42,
    'audio_files': 25,
    'video_files': 17,
    'total_size_mb': 1250.5
}
```

### Clear Cache
```bash
rm -rf local_cache/*
```

### Cache Persistence
- Cache persists across server restarts
- Files remain until manually deleted
- No automatic eviction (manage disk space externally)

## Production Deployment

```bash
# Create cache directory with proper permissions
mkdir -p /var/cache/media-streamer
chmod 755 /var/cache/media-streamer

# Run server with custom cache directory
CACHE_DIR=/var/cache/media-streamer python server.py

# Docker deployment
docker build -t media-streamer .
docker run -v $(pwd)/local_cache:/app/local_cache \
           -v $(pwd)/cookies.txt:/app/cookies.txt \
           -p 50051:50051 media-streamer
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Sign in to confirm" error | Update cookies.txt with fresh browser cookies |
| Slow first download | Normal - yt-dlp extraction + download time |
| Permission denied on cache | Ensure write permissions on `local_cache/` directory |
| Concurrent download conflicts | Server handles this automatically with locks |
| Disk space issues | Monitor and clear `local_cache/` periodically |

## License

MIT License
