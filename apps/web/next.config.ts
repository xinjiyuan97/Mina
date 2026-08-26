import type { NextConfig } from "next";

const serverUrl = process.env.MINA_SERVER_URL ?? "http://127.0.0.1:8787";

const nextConfig: NextConfig = {
  // The desktop browser may resolve the local app through 127.0.0.1 even when
  // Next starts on localhost. Without this, Next blocks client chunks and the
  // server-rendered composer never hydrates, leaving its send button disabled.
  allowedDevOrigins: ["localhost", "127.0.0.1"],
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${serverUrl}/api/:path*`,
      },
      {
        source: "/healthz",
        destination: `${serverUrl}/healthz`,
      },
    ];
  },
};

export default nextConfig;
