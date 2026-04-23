ARG GHCR_REGISTRY=ghcr.io
ARG GITHUB_REPOSITORY=xarantolus/groundstation
ARG TAG=latest

FROM ${GHCR_REGISTRY}/${GITHUB_REPOSITORY}/gs:${TAG}

COPY ax100_decoder.grc /tmp/ax100_decoder.grc

RUN cd /tmp && \
	grcc ax100_decoder.grc && \
	mv ax1ö00_decoder.py /usr/local/bin/ax100_decoder.py && \
	chmod +x /usr/local/bin/ax100_decoder.py

CMD ["ax100_decoder.py"]
