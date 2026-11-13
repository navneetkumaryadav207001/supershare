const activeStreams = new Map();

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// Receive chunk push commands from main app.js
self.addEventListener('message', (event) => {
  const { type, fileId, fileName, fileSize, chunk, offset } = event.data;

  if (type === 'REGISTER_STREAM') {
    let controllerRef;
    const stream = new ReadableStream({
      start(controller) {
        controllerRef = controller;
      }
    });

    activeStreams.set(fileId, {
      stream,
      controller: controllerRef,
      fileName,
      fileSize,
      nextExpectedOffset: 0,
      reorderBuffer: new Map() // Handles out-of-order packets from parallel channels
    });
  }

  if (type === 'PUSH_CHUNK') {
    const entry = activeStreams.get(fileId);
    if (!entry) return;

    // Store incoming chunk by byte offset and flush contiguous bytes in order
    entry.reorderBuffer.set(offset, new Uint8Array(chunk));

    while (entry.reorderBuffer.has(entry.nextExpectedOffset)) {
      const orderedChunk = entry.reorderBuffer.get(entry.nextExpectedOffset);
      entry.reorderBuffer.delete(entry.nextExpectedOffset);
      entry.controller.enqueue(orderedChunk);
      entry.nextExpectedOffset += orderedChunk.byteLength;
    }

    if (entry.nextExpectedOffset >= entry.fileSize) {
      entry.controller.close();
      activeStreams.delete(fileId);
    }
  }
});

// Intercept local HTTP GET /sw-stream/<fileId> and serve as a native file download
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (!url.pathname.includes('/sw-stream/')) return;

  const fileId = url.pathname.split('/sw-stream/')[1];
  const entry = activeStreams.get(fileId);
  if (!entry) {
    event.respondWith(new Response('Stream not found', { status: 404 }));
    return;
  }

  const headers = new Headers({
    'Content-Type': 'application/octet-stream',
    'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(entry.fileName)}`,
    'Content-Length': String(entry.fileSize),
    'Cache-Control': 'no-store'
  });

  event.respondWith(new Response(entry.stream, { status: 200, headers }));
});