FROM node:22-alpine AS build

WORKDIR /repo

COPY client-web/package.json client-web/package-lock.json ./client-web/
RUN cd client-web && npm ci

COPY client-web/ ./client-web/
RUN cd client-web && npm run build

COPY examples/basic/proto ./examples/basic/proto
COPY examples/basic/client/package.json examples/basic/client/package-lock.json ./examples/basic/client/

WORKDIR /repo/examples/basic/client
RUN npm pkg set dependencies.@grotto/client-web=file:../../../client-web
RUN npm install
COPY examples/basic/client/ping-client.ts ./
COPY examples/basic/client/gen ./gen/

RUN cp -rL node_modules/@grotto/client-web /tmp/client-web && \
    rm -rf node_modules/@grotto/client-web && \
    mv /tmp/client-web node_modules/@grotto/client-web

FROM node:22-alpine

WORKDIR /app

COPY --from=build /repo/examples/basic/client ./

ENV GROTTO_WS_URL=ws://server:50051

ENTRYPOINT ["npm", "start", "--"]
CMD ["ws://server:50051"]
