#!/bin/bash
# Compile Protocol Buffer files for MediaStreamer

echo "=========================================="
echo "Compiling media.proto..."
echo "=========================================="

# Check if grpc_tools is installed
if ! python -m grpc_tools.protoc --version &> /dev/null; then
    echo "❌ grpc_tools not found. Installing..."
    pip install grpcio-tools
fi

# Compile the proto file
python -m grpc_tools.protoc \
    --proto_path=. \
    --python_out=. \
    --grpc_python_out=. \
    media.proto

if [ $? -eq 0 ]; then
    echo "✅ Successfully generated:"
    echo "   📄 media_pb2.py (message definitions)"
    echo "   📄 media_pb2_grpc.py (gRPC service stubs)"
    echo ""
    echo "You can now run the server:"
    echo "   python server.py"
else
    echo "❌ Compilation failed"
    exit 1
fi
