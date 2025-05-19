#!/bin/bash

IMAGE_NAME="khoaihps1/airflow"
TAG="latest"

echo "🚧 Building Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} .

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "📤 Pushing image to Docker Hub..."
docker push ${IMAGE_NAME}:${TAG}

if [ $? -eq 0 ]; then
    echo "✅ Image pushed successfully to Docker Hub as ${IMAGE_NAME}:${TAG}"
else
    echo "❌ Failed to push image!"
    exit 1
fi
