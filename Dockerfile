FROM golang:1.24.1 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -o wagt ./cmd/main.go

FROM debian:bookworm-slim AS python-builder

# Install dependencies needed during download/bootstrap
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tar \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Download, extract, and bootstrap Astral standalone Python + hf_xet
RUN mkdir -p /agent/xet_python && \
    curl -sSL https://github.com/astral-sh/python-build-standalone/releases/download/20240107/cpython-3.10.13+20240107-x86_64-unknown-linux-gnu-install_only.tar.gz | tar -xz -C /agent/xet_python --strip-components=1 && \
    /agent/xet_python/bin/python3 -m pip install --upgrade pip && \
    /agent/xet_python/bin/python3 -m pip install hf_xet==1.5.2

# Bundle glibc runtime libraries so python3 works on any base image (including musl/Alpine).
# The bundled ld-linux loader invokes python3.10 with --library-path pointing to the bundled lib/.
RUN cp -L /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 \
         /lib/x86_64-linux-gnu/libc.so.6 \
         /lib/x86_64-linux-gnu/libpthread.so.0 \
         /lib/x86_64-linux-gnu/libdl.so.2 \
         /lib/x86_64-linux-gnu/libutil.so.1 \
         /lib/x86_64-linux-gnu/libm.so.6 \
         /lib/x86_64-linux-gnu/librt.so.1 \
         /lib/x86_64-linux-gnu/libgcc_s.so.1 \
         /lib/x86_64-linux-gnu/libnss_files.so.2 \
         /lib/x86_64-linux-gnu/libnss_dns.so.2 \
         /lib/x86_64-linux-gnu/libresolv.so.2 \
         /agent/xet_python/lib/ && \
    rm -f /agent/xet_python/bin/python3 && \
    printf '#!/bin/sh\nD="${0%%/*}"\nexec "${D}/../lib/ld-linux-x86-64.so.2" --library-path "${D}/../lib" "${D}/python3.10" "$@"\n' > /agent/xet_python/bin/python3 && \
    chmod +x /agent/xet_python/bin/python3

FROM alpine:3.19

COPY --from=builder /app/wagt /agent/wagt
COPY ./bin/ssh /agent/ssh
COPY --from=python-builder /agent/xet_python /agent/xet_python

RUN chmod -R +x /agent/wagt /agent/ssh /agent/xet_python/bin/

COPY <<EOF /agent/backend_dev.crt
-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUcNK7dtpLf7QnDMUK0isLDRlBe18wDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKbG9jYWxpcC1jYTAeFw0yNjA3MDEwMTExMjhaFw0zNjA2
MjgwMTExMjhaMBUxEzARBgNVBAMMCmxvY2FsaXAtY2EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCIaDGclU20NiTs0WE90cXyrEUtTCZgmrWnZcXNllCy
9EX+67DHpDA0QcdmQEclprl9sNeLanzEjf0TwM+WhMaawImHWBCYH1e4fm7+wPL5
Y8jObwYYBuXmt8P4M47vE+kKEMBLDxx2aitlpbRpAsqMoAsWD1XQ1HU34rDWCve+
7fmMAPo1t42+U947geaU9pZRje83iJO4mqs9kMH/QKd4z/93hAn2zXa/jmBRjpxk
187iFiQgJnyJWVwmxWqyb4p5zQDLwPZi/X72WYSu7+YQjrwQbW02N/MV9YMmgvi6
Lo08/VBRAeuJFI1GXqLDnnlg2O5FExcEPvYvDIlH0nflAgMBAAGjYzBhMB0GA1Ud
DgQWBBSkISilMdBxZW+e+oGL31kHfoxJwjAfBgNVHSMEGDAWgBSkISilMdBxZW+e
+oGL31kHfoxJwjAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjANBgkq
hkiG9w0BAQsFAAOCAQEABlKCf+RuMxTfan+08qvBKheozCHlKLsRYGV6pgElRFpS
e4YyZrXlXfnuZ4CDBA80K4Lq80+YWyvNuxHFOzvBx8cLuUkWwahufmqOFfuuYAFb
r7JV1Tkvz5qqjz53E4Np9nEj2ltorlpLpN/cNe7XzTjOEaQNcW92kzS5C7x3WCi9
Skj8B3FC30aNb+6pd5+WBut0o/z9NPs3npwLA/z7uWq4fFWgyHlNg/MTZvoYKLo1
musGdLIA8UTO4M0HjLBr2BpyxbDLUiSrDO0XXsQU/W3Ogs/d+d8JcR5XLEh5AmEc
ISKGDrVKkpB4nJWwLWW2L97AI1YoIkaiz6v/Huxk3w==
-----END CERTIFICATE-----
EOF

COPY <<EOF /agent/s3_dev.crt
-----BEGIN CERTIFICATE-----
MIIDITCCAgmgAwIBAgIUTt8K6LXKl5gfEhCRCfePSv2pHY8wDQYJKoZIhvcNAQEL
BQAwGDEWMBQGA1UEAwwNbG9jYWxpcC1zMy1jYTAeFw0yNjA3MDEwMTM2NDdaFw0z
NjA2MjgwMTM2NDdaMBgxFjAUBgNVBAMMDWxvY2FsaXAtczMtY2EwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQDj6J6R1DL+U2yd+whF1RhMSh0zFHTkj7Hd
/qfGk8VIW/PXVxC72QCMT8nAIe7quA00fNAGs1og1H6fvqV6JwW8lyv0CJyZKP4i
hMwnPfXrNZzWontVyWd4ivHqq2ocv6V8Cf7BJLeyqzKxMROg9/bP84iCin3KUp2z
YCNJJpyQMlo32TfiGob2szxMBWebhbxBVvAR9/wtW9AulvkLLhZJuUJ3Ou7AzwdQ
BGeRVEaBVZtYeedjuhAbYxEd3RqDk9n8B96AJb0sl4NzrM7onYWen2J83MTjxShc
xW9vJrFU5gvWPKujHLsh/jGS3DK7pXTgH0WcBHcupEBM7zMt7t8LAgMBAAGjYzBh
MB0GA1UdDgQWBBTHnWQB33tGEKQYsOvit1CD/ZPc4jAfBgNVHSMEGDAWgBTHnWQB
33tGEKQYsOvit1CD/ZPc4jAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIB
BjANBgkqhkiG9w0BAQsFAAOCAQEAcwpghPOfJbhGt1f7rTzppDS98uF6oPoPtLCa
znmGXvsA0TYtJK+JoqXweLniN31sgVejh3o7gprNUYm7trCj7u+tvvLbqKau3yf7
aIRzHqbXCuUKRnSz9jAtCeMlLQ1N1eumxl5kY2Y2HN9osimuUmElyQzOKG3nm4Om
AKm5DIHmAeT8nHcZ5ZMW7aLIhKh4onreOaERf6jG2G3xyTVAK3WAmE5zAm+AVTj4
QiCteefuVdBYglaICfAUXzL0u3AWagSspbCVDFZN6e5xeRPhkawO5QqRwaqQ6V68
lZ+Pbp8nH0ttPG2Jep2k2wfDukKJgF9Oz4NE07MdaGBNabhALA==
-----END CERTIFICATE-----
EOF

COPY <<EOF /agent/s3_iiasa.crt
-----BEGIN CERTIFICATE-----
MIIDtjCCAp4CCQDFm01lBlHbcDANBgkqhkiG9w0BAQsFADCBnDELMAkGA1UEBhMC
QVQxEDAOBgNVBAgMB0F1c3RyaWExEjAQBgNVBAcMCUxheGVuYnVyZzEOMAwGA1UE
CgwFSUlBU0ExDDAKBgNVBAsMA0lDVDEgMB4GA1UEAwwXY2VydGlmaWNhdGUuaWlh
c2EuYWMuYXQxJzAlBgkqhkiG9w0BCQEWGGljdC5oZWxwZGVza0BpaWFzYS5hYy5h
dDAeFw0yMzAzMjMxMzA3MDVaFw00MzEwMDQxMzA3MDVaMIGcMQswCQYDVQQGEwJB
VDEQMA4GA1UECAwHQXVzdHJpYTESMBAGA1UEBwwJTGF4ZW5idXJnMQ4wDAYDVQQK
DAVJSUFTQTEMMAoGA1UECwwDSUNUMSAwHgYDVQQDDBdjZXJ0aWZpY2F0ZS5paWFz
YS5hYy5hdDEnMCUGCSqGSIb3DQEJARYYaWN0LmhlbHBkZXNrQGlpYXNhLmFjLmF0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA08pA8TlPOhQ1rg2zBXgy
2ZOAPSB1GKsxuLhgqRh9MxBkfBKqqwbuvt2r/DFrqOccKY2njgKdwmxweqcp2T/H
hH756LOHiEZNvv6zBodpkYMF+VxSkepVTPIvNdHCFvy12c2uM4dL7pHhOqVBf6Ly
2wfmP/fj0mwJeRLx8wDvyMUkKf3kC6UTvT5AbK0LI6jeyLxJlzF6YQqGK6L52RS1
Pbnu4gIODHJHsNshg1QmBCQYI6v1L4FXgosNbksPf05wL2SB+DI/kktLP8qSXtkx
IV7WBPsilnu8R0md2wHL+WUNTwmukB2W6KlRqoqSgZJ3nNRaqnOq8HLU3FR0Fjn9
JwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAexeWy9rEH3x0SLK2D8VBgggIJv3iY
ZPeMMAotF9fop/+Tf4KrTs3tbs4mwDmg9dlxMNlAYvdOyC1mfSfg5qjCF71WRxY2
a+9sIb2rvmaQ5pEuO8i7RGgTOeHj5E7f8UoCwRnC+JUw52eOTjcCfw1QxoWGieiB
whrNbNhjI0xWDNxLb2VZ0rfFtO6lEFzVQbF6GIXq4QOjxtWRV/DQKX+S4aZgmniT
0vTP1bVoS1vHkidVAFZ9v82pCGZFXpjku/gjjmO4Yc/In/WeqiyKZ2HRzIO/ZcGk
nP6/j/YnBT9ayxJE5ku2OXNh/EiuNZRytdImcik6K4TePQjhvP4gXmK5
-----END CERTIFICATE-----
EOF

RUN cat /etc/ssl/certs/ca-certificates.crt \
        /agent/backend_dev.crt \
        /agent/s3_dev.crt \
        /agent/s3_iiasa.crt \
    > /agent/ca_bundle.pem

RUN chmod -R a+rX /agent && chown -R 65534:65534 /agent

# Recursively copy all bundled agent executables and python virtual environment to the shared volume
ENTRYPOINT ["sh", "-c", "mkdir -p /mnt/tmp/.wkube_agent && cp -rp /agent/* /mnt/tmp/.wkube_agent/"]