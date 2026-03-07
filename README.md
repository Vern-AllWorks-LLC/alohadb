AlohaDB - AI-Native PostgreSQL
==============================

AlohaDB is an AI-native database built on PostgreSQL 18. It extends
PostgreSQL with 34 purpose-built extensions for modern application
development including real-time change notifications, time-travel
queries, columnar storage, full-text search, GraphQL, natural language
to SQL, encrypted audit logging, and more.

Copyright and license information can be found in the files COPYRIGHT
(PostgreSQL base) and LICENSE (AlohaDB extensions - MIT).

Building from Source
--------------------

    meson setup builddir --prefix=/var/lib/alohadb -Drpath=true
    ninja -C builddir -j$(nproc)
    ninja -C builddir install

For detailed documentation, see the AlohaDB User Guide at <https://www.opencan.ai/alohadb-guide.html> or the
PostgreSQL documentation at <https://www.postgresql.org/docs/18/>.

For more information visit <https://opencan.ai/>.
