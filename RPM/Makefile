SPECFILE    = $(shell find -maxdepth 1 -type f -name *.spec)
PACKAGE     = $(shell awk '$$1 == "Name:"     { print $$2 }' $(SPECFILE) )
RELEASE     = $(shell awk '$$1 == "Release:"  { print $$2 }' $(SPECFILE) )
TARFILE     = $(PACKAGE)-$(RPM_VERSION).tar.gz
RPMTOPDIR   = $(shell rpm --eval '%{_topdir}')
BUILDARCH   = $(shell awk '$$1 == "BuildArch:"  { print $$2 }' $(SPECFILE) )
BUILTSRPM   = $(RPMTOPDIR)/SRPMS/$(PACKAGE)-$(RPM_VERSION)-$(RELEASE).src.rpm
BUILTRPM    = $(RPMTOPDIR)/RPMS/$(BUILDARCH)/$(PACKAGE)-$(RPM_VERSION)-$(RELEASE).$(BUILDARCH).rpm

all: $(TARFILE)

$(TARFILE):
	sed -i 's/^Version: .*$$/Version: $(RPM_VERSION)/g' $(SPECFILE)
	tar cvzf $(TARFILE) --dereference --exclude-vcs --transform 's,^,$(PACKAGE)-$(RPM_VERSION)/,' *

clean:
	rm $(TARFILE)

build: $(TARFILE) $(SPECFILE)
	mv $(TARFILE) $(RPMTOPDIR)/SOURCES
	rpmbuild -ba $(SPECFILE)
	mkdir -p build/$(BUILDARCH)
	cp $(BUILTSRPM) build/
	cp $(BUILTRPM) build/$(BUILDARCH)