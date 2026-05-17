FROM golang:tip AS build

WORKDIR /build

COPY ./ ./

WORKDIR /build/examples/basic/server

RUN CGO_ENABLED=0 go build -o server .

FROM scratch

COPY --from=build /build/examples/basic/server/server /server

CMD ["/server"]
