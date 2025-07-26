FROM alpine:latest

RUN apk add --no-cache \
	ffmpeg

COPY waterfall.sh /usr/local/bin/waterfall.sh
RUN chmod +x /usr/local/bin/waterfall.sh

ENTRYPOINT [ "/usr/local/bin/waterfall.sh" ]
