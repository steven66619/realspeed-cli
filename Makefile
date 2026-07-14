# realspeed-cli Makefile
# Build and install a Virgin Media SamKnows speed test CLI

BINARY  = realspeed-cli
PREFIX  ?= /usr/local
DESTDIR ?=

all: $(BINARY)

$(BINARY): main.go
	go build -ldflags="-s -w" -o $@ $<

install: $(BINARY)
	install -Dm755 $(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(BINARY)

clean:
	rm -f $(BINARY)

.PHONY: all install uninstall clean
