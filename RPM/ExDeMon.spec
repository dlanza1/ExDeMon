Summary: ExDeMon - export, define and monitor metrics
Name: exdemon
Version: no-version
Release: 1.0
License: GPL
Buildroot: %{_tmppath}/%{name}-%{version}
Source: %{name}-%{version}.tar.gz
BuildArch: noarch

%description
ExDeMon is a general purpose metrics monitor implemented with Apache Spark. Kafka source, Elastic sink, aggregate metrics, different analysis, notifications, actions, live configuration update, missing metrics, ...

%prep
%setup -n %{name}-%{version}

%build

%install
[ -d %{buildroot} ] && rm -rf %{buildroot}

mkdir -p %{buildroot}/opt/%{name}/lib/
install -m 644 lib/exdemon-*.jar %{buildroot}/opt/%{name}/lib/

mkdir -p %{buildroot}/opt/%{name}/bin/
install -m 744 bin/* %{buildroot}/opt/%{name}/bin/

mkdir -p %{buildroot}/opt/%{name}/etc/

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
/opt/%{name}/*
/etc/cron.d/%{name}.cron

%post

%preun

%postun

%changelog
* Wed Nov 15 2017 daniel.lanza (at) cern.ch - 0.1-0.1
- Initial packaged version