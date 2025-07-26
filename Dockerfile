FROM fedora:42

WORKDIR /app

RUN --mount=type=cache,target=/var/cache/dnf \
	dnf update -y --setopt=keepcache=1 && \
	dnf install -y --setopt=keepcache=1 \
	gnuradio \
	gnuradio-devel \
	gtk3 \
	libX11 \
	xorg-x11-xauth \
	python3-gobject \
	gobject-introspection \
	gtk3-devel \
	make \
	gcc \
	gcc-c++ \
	git \
	cmake \
	wget \
	pkgconf \
	libvorbis-devel \
	hamlib-devel \
	hamlib \
	hamlib-c++ \
	hamlib-c++-devel \
	libpng-devel \
	nlohmann-json-devel \
	python3-hamlib \
	libcanberra-gtk3 \
	libcanberra-devel \
	spdlog-devel \
	gmp gmp-devel \
	fftw-devel \
	blas-devel \
	lapack-devel \
	pybind11-devel \
	python3-devel \
	xterm

 RUN	cd /tmp && \
	wget https://download.savannah.nongnu.org/releases/pngpp/png++-0.2.9.tar.gz && \
	tar -xzf png++-0.2.9.tar.gz && \
	cp -r png++-0.2.9 /usr/include/png++ && \
	rm -rf png++-0.2.9*

RUN wget -O /tmp/itpp-4.3.1.tar.bz2 https://sourceforge.net/projects/itpp/files/itpp/4.3.1/itpp-4.3.1.tar.bz2

RUN --mount=type=cache,target=/tmp/itpp-4.3.1 \
	cd /tmp && \
	tar -xjf itpp-4.3.1.tar.bz2 && \
	cd itpp-4.3.1 && \
	mkdir -p build && cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	make -j$(nproc) && \
	make install && \
	ldconfig

RUN --mount=type=cache,target=/tmp/gr-satnogs \
	if [ -d /tmp/gr-satnogs/.git ]; then \
	cd /tmp/gr-satnogs && git pull; \
	else \
	git clone https://gitlab.com/librespacefoundation/satnogs/gr-satnogs.git --recurse-submodules /tmp/gr-satnogs; \
	fi && \
	cd /tmp/gr-satnogs && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	make -j$(nproc) && \
	make install && \
	ldconfig

RUN --mount=type=cache,target=/tmp/rtl-sdr-blog \
	--mount=type=cache,target=/var/cache/dnf \
	dnf install -y libusb1-devel && \
	if [ -d /tmp/rtl-sdr-blog/.git ]; then \
	cd /tmp/rtl-sdr-blog && git pull; \
	else \
	git clone https://github.com/rtlsdrblog/rtl-sdr-blog.git /tmp/rtl-sdr-blog; \
	fi && \
	cd /tmp/rtl-sdr-blog && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release -DINSTALL_UDEV_RULES=ON .. && \
	make -j$(nproc) && \
	make install && \
	cp ../rtl-sdr.rules /etc/udev/rules.d/ && \
	ldconfig

RUN --mount=type=cache,target=/tmp/gr-osmosdr \
	if [ -d /tmp/gr-osmosdr/.git ]; then \
	cd /tmp/gr-osmosdr && git pull; \
	else \
	git clone https://gitea.osmocom.org/sdr/gr-osmosdr.git /tmp/gr-osmosdr; \
	fi && \
	cd /tmp/gr-osmosdr && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	make -j$(nproc) && \
	make install && \
	ldconfig

COPY auto/requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
	pip3 install matplotlib numpy scipy && \
	pip3 install -r /tmp/requirements.txt && \
	rm -rf /tmp/requirements.txt

# Ensures that the osmosdr .so files are found
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

# Disables the annoying message GNU Radio shows on every first run
RUN echo -e "[grc]\nxterm_executable=/usr/bin/xterm" >> /etc/gnuradio/conf.d/grc.conf

CMD ["gnuradio-companion"]
