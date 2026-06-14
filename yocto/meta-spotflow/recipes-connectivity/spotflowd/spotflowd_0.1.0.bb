SUMMARY = "Spotflow observability daemon"
DESCRIPTION = "Collects logs (syslog, journald) and OS metrics from embedded \
Linux devices and streams them to the Spotflow platform over MQTT/TLS."
HOMEPAGE = "https://spotflow.io"
LICENSE = "BSL-1.1"
LIC_FILES_CHKSUM = "file://LICENSE.MD;md5=FIXME"

SRC_URI = " \
    git://github.com/kucerah0nza/spotflowd.git;protocol=https;branch=main \
    file://spotflowd.init \
"

SRCREV = "${AUTOREV}"
S = "${WORKDIR}/git"
PV = "0.1.0+git"

inherit cargo useradd update-rc.d

# ---------------------------------------------------------------------------
# Cargo / Rust
# ---------------------------------------------------------------------------

# Build without journald by default — most Yocto images lack systemd.
CARGO_BUILD_FLAGS += "--no-default-features"

# Optional: enable journald support on systemd-based Yocto images.
PACKAGECONFIG[journald] = "--features journald,,systemd"

# Apply PACKAGECONFIG feature flags to the Cargo build.
CARGO_BUILD_FLAGS += "${@' '.join(d.getVar('PACKAGECONFIG_CONFARGS').split())}"

# ---------------------------------------------------------------------------
# Users / groups
# ---------------------------------------------------------------------------

USERADD_PACKAGES = "${PN}"
USERADD_PARAM:${PN} = "-r -s /bin/false -d /var/lib/spotflow spotflow"
GROUPADD_PARAM:${PN} = "-r spotflow"

# ---------------------------------------------------------------------------
# SysVinit
# ---------------------------------------------------------------------------

INITSCRIPT_NAME = "spotflowd"
INITSCRIPT_PARAMS = "defaults 85 15"

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

do_install() {
    # Binary
    install -d ${D}${sbindir}
    install -m 0755 ${CARGO_TARGET_DIR}/${CARGO_TARGET_SUBDIR}/spotflowd ${D}${sbindir}/spotflowd

    # Config — install the example, then apply Yocto-appropriate defaults:
    # syslog on, journald off, and use /var/log/messages (BusyBox syslogd default).
    install -d ${D}${sysconfdir}/spotflow
    install -m 0600 ${S}/config/spotflowd.toml.example ${D}${sysconfdir}/spotflow/spotflowd.toml
    sed -i 's/^journald = true/journald = false/' ${D}${sysconfdir}/spotflow/spotflowd.toml
    sed -i 's/^syslog = false/syslog = true/'     ${D}${sysconfdir}/spotflow/spotflowd.toml
    sed -i 's|^syslog_path = "/var/log/syslog"|syslog_path = "/var/log/messages"|' ${D}${sysconfdir}/spotflow/spotflowd.toml

    # SysVinit script
    install -d ${D}${sysconfdir}/init.d
    install -m 0755 ${WORKDIR}/spotflowd.init ${D}${sysconfdir}/init.d/spotflowd

    # Spool directory
    install -d ${D}/var/lib/spotflow/spool
    chown spotflow:spotflow ${D}/var/lib/spotflow
    chown spotflow:spotflow ${D}/var/lib/spotflow/spool

    # Runtime directory for the custom metrics socket
    install -d ${D}/var/run/spotflow
}

# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------

# Preserve user-edited config across upgrades.
CONFFILES:${PN} = "${sysconfdir}/spotflow/spotflowd.toml"

# Directories owned by this package.
FILES:${PN} += " \
    ${sbindir}/spotflowd \
    ${sysconfdir}/spotflow \
    ${sysconfdir}/init.d/spotflowd \
    /var/lib/spotflow \
    /var/run/spotflow \
"

RDEPENDS:${PN} += "ca-certificates"
