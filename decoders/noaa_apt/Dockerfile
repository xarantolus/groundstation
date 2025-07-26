ARG GHCR_REGISTRY=ghcr.io
ARG GITHUB_REPOSITORY=xarantolus/groundstation
ARG TAG=latest

FROM ${GHCR_REGISTRY}/${GITHUB_REPOSITORY}/gs:${TAG}

COPY noaa_apt_decoder.grc /tmp/noaa_apt_decoder.grc

RUN cd /tmp && \
	grcc noaa_apt_decoder.grc && \
	mv noaa_apt_decoder.py /usr/local/bin/noaa_apt_decoder.py && \
	chmod +x /usr/local/bin/noaa_apt_decoder.py

CMD ["noaa_apt_decoder.py"]
