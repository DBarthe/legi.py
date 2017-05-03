# -*- coding: utf-8 -*-

from argparse import ArgumentParser
# from cassandra.cluster import Cluster
from datetime import datetime
from elasticsearch import Elasticsearch

from .export import iterate_everything, iterate_texte, iterate_cid
from .utils import connect_db


class Handlers:
    def __init__(self):
        self.funs = {}

    def on(self, event, fun):
        self.funs[event] = fun

    def notify(self, event, *arg, **kwarg):
        if event in self.funs:
            self.funs[event](*arg, **kwarg)
        else:
            print "unknown event:", event


def iterate(stream, handlers):
    for i, t in enumerate(stream):
        if args.limit is not None and i >= args.limit:
            print('reached the limit (%i)' % args.limit)
            return
        e_type, e_payload = t
        handlers.notify(e_type, e_payload)
    print('end')
    print('%i elements listed' % i)


def convert_field_to_date(s, key=None):
    try:
        return datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        return s  # do not modify


def key_is_date_field(key):
    return ("date" in key or "fin" in key or "debut" in key) and key not in ["page_fin_publi"]


def doc_cleanup(doc):
    for key in doc:
        if isinstance(doc[key], dict):
            doc[key] = doc_cleanup(doc[key])
        elif key_is_date_field(key):
            doc[key] = convert_field_to_date(doc[key], key)
    return doc


def main(args):
    db = connect_db(args.sqlite)
    # cluster = Cluster([args.elassandra])
    es = Elasticsearch([args.elassandra])

    if args.texte:
        if not args.cid:
            raise SystemExit("--texte nécessite --cid")
        texte_id = db.one("SELECT texte_id FROM textes_versions WHERE cid = ? LIMIT 1", (args.cid,))
        stream = iterate_texte(db, texte_id)
    elif args.cid:
        stream = iterate_cid(db, args.cid)
    else:
        stream = iterate_everything(db)

    ctx = {}
    handlers = Handlers()

    def handle_texte_version(e_payload):
        ctx['texte_version'] = doc_cleanup(e_payload)

    def handle_section(e_payload):
        ctx['section'] = doc_cleanup({
            'meta': e_payload[0],
            'content': e_payload[1],
        })

    def handle_article(e_payload):
        ctx['article'] = doc_cleanup({
            'meta': e_payload[0],
            'content': e_payload[1],
        })
        print ctx
        es.index(index="legi", doc_type='code_civil', id=ctx['article']['content']['id'], body=ctx)

    handlers.on("texte_version", handle_texte_version)
    handlers.on("section", handle_section)
    handlers.on("article", handle_article)

    iterate(stream, handlers)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('sqlite')
    p.add_argument('elassandra')
    p.add_argument('limit', type=int)
    p.add_argument('--cid', nargs='?')
    p.add_argument('--texte', action='store_true', default=False,
                   help="active l'export de toutes les versions du texte identitifé par --cid")
    args = p.parse_args()
    main(args)
