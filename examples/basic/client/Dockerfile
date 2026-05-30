FROM node:22-alpine AS build

WORKDIR /repo

COPY package.json yarn.lock ./
COPY client-web/package.json ./client-web/
COPY examples/basic/client/package.json ./examples/basic/client/

RUN yarn install --frozen-lockfile

COPY client-web/ ./client-web/
RUN yarn workspace @grotto-web/client-web build

COPY examples/basic/proto ./examples/basic/proto
COPY examples/basic/client/src ./examples/basic/client/src

FROM node:22-alpine

WORKDIR /app

COPY --from=build /repo ./

WORKDIR /app/examples/basic/client

ENV GROTTO_URL=http://server:50051

ENTRYPOINT ["yarn", "start"]
