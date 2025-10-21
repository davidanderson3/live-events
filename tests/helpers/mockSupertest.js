import http from 'node:http';
import { PassThrough } from 'node:stream';

function normalizeHeaders(headers = {}) {
  return Object.entries(headers).reduce((acc, [key, value]) => {
    const normalizedKey = String(key).toLowerCase();
    acc[normalizedKey] = value;
    return acc;
  }, {});
}

function parseBody(buffer, headers = {}) {
  if (!buffer || !buffer.length) {
    return null;
  }
  const contentType = String(headers['content-type'] || headers['content-type'] || '').toLowerCase();
  if (contentType.includes('application/json')) {
    try {
      return JSON.parse(buffer.toString('utf8'));
    } catch {
      return buffer.toString('utf8');
    }
  }
  return buffer;
}

function performRequest(app, { method, path, headers = {}, body }) {
  return new Promise((resolve, reject) => {
    const normalizedHeaders = normalizeHeaders(headers);
    if (!normalizedHeaders.host) {
      normalizedHeaders.host = 'localhost';
    }

    const reqStream = new PassThrough();
    const req = new http.IncomingMessage(reqStream);
    req.method = method.toUpperCase();
    req.url = path;
    req.headers = normalizedHeaders;

    const resStream = new PassThrough();
    const res = new http.ServerResponse(req);
    res.assignSocket(resStream);

    resStream.resume();
    resStream.on('error', reject);
    res.on('error', reject);

    const chunks = [];
    const originalWrite = res.write.bind(res);
    const originalEnd = res.end.bind(res);

    res.write = (chunk, encoding, callback) => {
      if (chunk) {
        chunks.push(Buffer.from(chunk, typeof encoding === 'string' ? encoding : undefined));
      }
      return originalWrite(chunk, encoding, callback);
    };

    res.end = (chunk, encoding, callback) => {
      if (chunk) {
        chunks.push(Buffer.from(chunk, typeof encoding === 'string' ? encoding : undefined));
      }
      return originalEnd(chunk, encoding, callback);
    };

    let finished = false;
    const finalize = () => {
      if (finished) return;
      finished = true;
      const buffer = chunks.length ? Buffer.concat(chunks) : Buffer.alloc(0);
      const text = buffer.toString('utf8');
      const bodyResult =
        parseBody(buffer, res.getHeaders()) ??
        (text.length ? text : null);
      resolve({
        status: res.statusCode,
        statusCode: res.statusCode,
        ok: res.statusCode >= 200 && res.statusCode < 400,
        headers: res.getHeaders(),
        text,
        body: bodyResult
      });
    };

    resStream.on('end', finalize);
    res.on('finish', finalize);

    if (body !== undefined) {
      let payload = body;
      if (!(payload instanceof Buffer) && typeof payload !== 'string') {
        if (!normalizedHeaders['content-type']) {
          normalizedHeaders['content-type'] = 'application/json';
        }
        payload = JSON.stringify(payload);
      }
      reqStream.end(payload);
    } else {
      reqStream.end();
    }

    try {
      app(req, res);
    } catch (err) {
      reject(err);
    }
  });
}

class PendingRequest {
  constructor(app, method, path) {
    this.app = app;
    this.method = method;
    this.path = path;
    this.headers = {};
    this.payload = undefined;
    this._promise = null;
  }

  send(body) {
    this.payload = body;
    return this;
  }

  set(name, value) {
    this.headers[name] = value;
    return this;
  }

  exec() {
    if (!this._promise) {
      this._promise = performRequest(this.app, {
        method: this.method,
        path: this.path,
        headers: this.headers,
        body: this.payload
      });
    }
    return this._promise;
  }

  then(onFulfilled, onRejected) {
    return this.exec().then(onFulfilled, onRejected);
  }

  catch(onRejected) {
    return this.exec().catch(onRejected);
  }

  finally(onFinally) {
    return this.exec().finally(onFinally);
  }
}

class RequestBuilder {
  constructor(app) {
    this.app = app;
  }

  get(path) {
    return new PendingRequest(this.app, 'GET', path);
  }

  post(path) {
    return new PendingRequest(this.app, 'POST', path);
  }

  put(path) {
    return new PendingRequest(this.app, 'PUT', path);
  }

  delete(path) {
    return new PendingRequest(this.app, 'DELETE', path);
  }
}

export default function request(app) {
  return new RequestBuilder(app);
}

export { request };
