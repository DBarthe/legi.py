from argparse import ArgumentParser
import sys

from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine import management
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

from legi.export import iterate_texte, iterate_cid, iterate_everything
from .utils import connect_db


class ArticleModel(Model):
    __table_name__ = "article"

    id = columns.Text(primary_key=True)
    bloc_textuel = columns.Text()
    cid = columns.Text()
    etat = columns.Text()
    dossier = columns.Text()
    date_fin = columns.Text()
    date_debut = columns.Text()
    nota = columns.Text()
    section = columns.Text()
    num = columns.Text()
    mtime = columns.Integer()
    type = columns.Text()


class Exporter():
    def __init__(self):
        self.current_texte_version = None
        self.current_section = None

    def process_item(self, e_type, e_payload):
        if e_type == "texte_version":
            self.current_texte_version = e_payload
            self.export_texte_version(e_payload)
        elif e_type == "section":
            self.current_section = e_payload
            self.export_section(e_payload[0], e_payload[1])
        elif e_type == "article":
            self.current_section = e_payload
            self.export_article(e_payload[0], e_payload[1])
        else:
            print("unknown element type %s" % e_type, file=sys.stderr)

    def export_texte_version(self, texte_version):
        pass

    def export_section(self, sommaire, section):
        pass

    def export_article(self, sommaire, article):
        pass


class CQLExporter(Exporter):
    def __init__(self, endpoints, keyspace):
        super().__init__()
        connection.setup(endpoints, keyspace, protocol_version=3)
        management.create_keyspace_simple(keyspace, 1)
        sync_table(ArticleModel, keyspaces=[keyspace])

    def export_article(self, sommaire, article):
        ArticleModel.create(**article)


def main(args):
    db = connect_db(args.db)
    exporter = CQLExporter(args.endpoints, args.keyspace)

    if args.texte:
        if not args.cid:
            raise SystemExit("--texte nécessite --cid")
        texte_id = db.one("SELECT texte_id FROM textes_versions WHERE cid = ? LIMIT 1", (args.cid,))
        stream = iterate_texte(db, texte_id)
    elif args.cid:
        stream = iterate_cid(db, args.cid)
    else:
        stream = iterate_everything(db)
    for i, t in enumerate(stream):
        if i >= args.limit:
            print('reached the limit (%i)' % args.limit)
            return
        e_type, e_payload = t
        exporter.process_item(e_type, e_payload)
    print('end')
    print('%i elements listed' % i)


if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('db')
    p.add_argument('limit', type=int)
    p.add_argument('--cid', nargs='?')
    p.add_argument('--texte', action='store_true', default=False,
                   help="active l'export de toutes les versions du texte identitifé par --cid")
    p.add_argument('--endpoints', nargs='+', required=True)
    p.add_argument("--keyspace", nargs="?", default="legi")
    args = p.parse_args()
    main(args)
