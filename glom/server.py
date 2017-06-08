import gevent.monkey
gevent.monkey.patch_all()

import ctypes
import gzip
import hashlib
import os
import shutil
import threading
import uuid
import bottle
import magic
import pymongo
import requests
import simplejson as json


CONFIG_FILE_NAME = 'glom_config.json'
REQUIRED_FIELDS = ['height', 'media_type', 'src', 'tags', 'title', 'username',
    'width']

class GlomServer():
    def __init__(self):
        self.config()
        self.db = pymongo.MongoClient(self.opt.mongo_uri).glom
        self.create_app()
        self.nonce = uuid.uuid4().hex

    def config(self):
        conf = self.read_config()
        if conf is None:
            raise RuntimeError('No configuration file provided.')
        self.opt = lambda: None
        for k, v in conf.items():
            setattr(self.opt, k, v)

    def read_config(self):
        conf = None
        try:
            with open(
                os.path.join(
                    os.path.dirname(__file__),
                    CONFIG_FILE_NAME
                ), 'r'
            ) as f:
                conf = json.load(f)
        except FileNotFoundError:
            print('File Not Found - Expected file {0} was not found.'.format(
                os.path.join(os.path.dirname(__file__), CONFIG_FILE_NAME)
            ))
        finally:
            return conf

    def run(self):
        self.server_thread = threading.Thread(
            target= self.app.run,
            kwargs= {
                'host': self.opt.host,
                'port': self.opt.port,
                'server': GeBottle_Server,
                'cert': self.opt.ssl_cert,
                'key': self.opt.ssl_key
            }
        )
        self.server_thread.start()

    def stop(self):
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self.server_thread.ident),
            ctypes.py_object(KeyboardInterrupt)
        )
        self.server_thread.join()

    def begin_download(self, data):
        chunk_size = 1024 * 1024 if data['media_type'] == 'video' else 1024
        r = requests.get(data['src'], stream= True)
        with open(os.path.join(self.opt.storage_path, data['filename']), 'wb') as f:
            for chunk in r.iter_content(chunk_size):
                f.write(chunk)
        return os.path.join(self.opt.storage_path, data['filename'])


    def glom_media(self, data):
        thr = threading.Thread(
            target= self.download_media,
            args= (data, ),
            daemon= True
        )
        thr.start()

    def download_media(self, data):
        file_location = self.begin_download(data)
        filehash = self.get_file_fingerprint(file_location)
        mime_type = magic.from_file(file_location, mime= True)
        same_file = self.db.media.find_one({'fingerprint': filehash})
        if same_file:
            #change the record to point to the existing file and delete new one
            self.db.media.update_one(
                {'filename': data['filename']},
                {'$set':{
                    'filename': same_file['filename'],
                    'fingerprint': filehash,
                    'mime_type': mime_type,
                    'processed': True
                }}
            )
            os.remove(file_location)
        else:
            #update the existing record with the data
            self.db.media.update_one(
                {'filename': data['filename']},
                {'$set': {
                    'fingerprint': filehash,
                    'mime_type': mime_type,
                    'processed': True
                    }
                }
            )

    def get_file_fingerprint(self, file_path):
        blocksize = 65536
        hsh = hashlib.md5()
        try:
            f = gzip.open(file_path, 'rb')
            buf = f.read(blocksize)
        except OSError:
            f = open(file_path, 'rb')
            buf = f.read(blocksize)
        while len(buf) > 0:
            hsh.update(buf)
            buf = f.read(blocksize)
        f.close()
        return hsh.hexdigest()

    def create_app(self):
        self.app = bottle.Bottle()

        @self.app.get('/')
        def user_home():
            cookie = bottle.request.get_cookie('glom_credentials')
            if cookie:
                if cookie == self.nonce:
                    return bottle.static_file('user_home.html',
                        root= os.path.join(
                            os.path.join(os.path.dirname(__file__), 'assets'),
                            'html'
                        )
                    )
                else:
                    bottle.abort(401, 'Token not recognized.')
            else:
                bottle.abort(401, 'Token not provided.')

        @self.app.get('/handshake')
        @json_head
        def send_nonce():
            return {'token': self.nonce}

        @self.app.get('/tags/<user>')
        @json_head
        def get_tags(user):
            tags = self.db.media.distinct('tags', filter= {'username': user})
            if tags is None:
                tags = []
            return {'tags': tags}

        @self.app.get('/user_media/<user>')
        @json_head
        def get_user_media_list(user):
            media = [item for item in self.db.media.find(
                {'username': user, 'processed': True},
                {
                    '_id': 0,
                    'filename': 1,
                    'height': 1,
                    'media_type': 1,
                    'mime_type': 1,
                    'tags': 1,
                    'title': 1,
                    'width': 1
                }
            )]
            return {'media_list': media}

        @self.app.get('/media/<file_id>')
        def get_item(file_id):
            mime_type= self.db.media.find_one(
                {'filename': file_id}
            )['mime_type']
            return bottle.static_file(file_id,
                root= self.opt.storage_path,
                mimetype= mime_type
            )

        @self.app.get('/assets/<filepath:path>')
        def get_static_asset(filepath):
            return bottle.static_file(filepath,
                root= os.path.join(os.path.dirname(__file__), 'assets')
            )

        @self.app.post('/media')
        def add_item():
            data = bottle.request.json
            if data is None:
                bottle.abort(415, 'Content-Type must be application/json')
            try:
                validate_item(data)
            except ItemValidationException as e:
                 #inform application about error
                bottle.abort(400, repr(e))
            new_id = uuid.uuid4().hex
            doc = {
                'filename': new_id,
                'fingerprint': None,
                'height': data['height'],
                'media_type': data['media_type'],
                'mime_type': None,
                'processed': False,
                'src': data['src'],
                'tags': data['tags'],
                'title': data['title'],
                'username': data['username'],
                'width': data['width']
            }
            self.db.media.insert_one(doc)
            self.glom_media(doc)



class GeBottle_Server(bottle.ServerAdapter):
    def __init__(self, host= '127.0.0.1', port= '8080', cert= None, key= None, **options):
        super().__init__(host, port, **options)
        self.ssl_key = key
        self.ssl_cert = cert

    def run(self, handler):
        from gevent import pywsgi, local
        if not isinstance(threading.local(), local.local):
            raise RuntimeError('Bottle requires gevent.monkey.patch_all')
        pywsgi.WSGIServer(
            (self.host, self.port),
            handler,
            keyfile= self.ssl_key,
            certfile= self.ssl_cert
        ).serve_forever()


class ItemValidationException(BaseException):
    pass


def json_head(func):
    def json_header_content(*args, **kwargs):
        bottle.response.set_header('Content-Type', 'application/json')
        return func(*args, **kwargs)
    return json_header_content

def validate_item(data):
    for field in REQUIRED_FIELDS:
        if field not in data:
            raise ItemValidationException(
                "Expected required field - '{0}'".format(field)
            )



if __name__ == '__main__':
    x = GlomServer()
    x.run()
