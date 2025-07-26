### Permanent OS Settings
Basically followed the instructions [for the RTL-SDR Blog V4 here](https://www.rtl-sdr.com/V4/), which also disables certain drivers. Basically, run the following command and reboot:

```
echo 'blacklist dvb_usb_rtl28xxu' | sudo tee --append /etc/modprobe.d/blacklist-dvb_usb_rtl28xxu.conf
```

### Setup

```
sudo apt update && sudo apt upgrade -y && sudo apt-get install -y podman zstd python3 python3-pip rtl-sdr
```
