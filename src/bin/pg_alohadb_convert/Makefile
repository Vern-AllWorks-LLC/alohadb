#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_alohadb_convert
#
# Conversion tools for clusters and scripts from AlohaDB 20.x.
#
# src/bin/pg_alohadb_convert/Makefile
#
#-------------------------------------------------------------------------

subdir = src/bin/pg_alohadb_convert
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

GENERATED = pg_alohadb_convert_sql pg_alohadb_convert_db pg_alohadb_convert_db.sql
SOURCES = generate.py rename_map.json common.py.in pg_alohadb_convert_sql.in \
	pg_alohadb_convert_db.in pg_alohadb_convert_db.sql.in

all: $(GENERATED)

$(GENERATED): $(addprefix $(srcdir)/,$(SOURCES))
	$(PYTHON) $(srcdir)/generate.py $(srcdir) .

install: all installdirs
	$(INSTALL_SCRIPT) pg_alohadb_convert_sql '$(DESTDIR)$(bindir)/pg_alohadb_convert_sql'
	$(INSTALL_SCRIPT) pg_alohadb_convert_db '$(DESTDIR)$(bindir)/pg_alohadb_convert_db'
	$(INSTALL_DATA) pg_alohadb_convert_db.sql '$(DESTDIR)$(datadir)/pg_alohadb_convert_db.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_alohadb_convert_sql' \
	      '$(DESTDIR)$(bindir)/pg_alohadb_convert_db' \
	      '$(DESTDIR)$(datadir)/pg_alohadb_convert_db.sql'

clean distclean:
	rm -f $(GENERATED)
