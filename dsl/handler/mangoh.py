from dsl import json

import pymongo
from bson import json_util


class MongoHandler(object):

    hkey = 'MongoHandler'

    action_method_map = {
        'find':'_find',
        'insert':'_insert',
        'update':'_update',
        'delete':'_delete',
        'find_and_modify':'_find_and_modify',
    }

    args = {
        'host':'localhost', 
        'port':27017, 
    }

    
    def _get_client(self):
        if not getattr(self, '_client', None):
            print('init client')
            self._client = pymongo.MongoClient(**self.args)

        return self._client

    
    
    #@asyn.task
    def action(self, step, param):
        print(step)

        actiontype = step['actiontype']
        database = step['database']
        template_raw = step['template']

        if isinstance(template_raw, str):
            template = json.loads(template_raw % param)
        else:
            template = template_raw

        if not actiontype or actiontype not in self.action_method_map:
            raise ValueError('Invalid actiontype %s' % actiontype)
        else:
            method = getattr(self, self.action_method_map[actiontype])

        return method(database, template, param)


    
    def _find(self, database, template, param=None):
        collection = template['find'] if 'find' in template else None
        query = template['query'] if 'query' in template else {}
        projection = template['projection'] if 'projection' in template else None
        skip = template['skip'] if 'skip' in template else 0
        limit = template['limit'] if 'limit' in template else 0
        sort = template['sort'] if 'sort' in template else None
        exhaust = template['exhaust'] if 'exhaust' in template else False

        cursor = self._get_client()[database][collection].find(
            spec=query, 
            fields=projection,
            skip=skip,
            limit=limit,
            sort=sort,
            exhaust=exhaust
            #callback=self._on_result,
        )

        result_json = json_util.dumps(cursor)

        return {'value':result_json}


    
    def _insert(self, database, template, param=None):
        collection = template['insert'] if 'insert' in template else None
        documents = template['documents'] if 'documents' in template else []

        kwargs = {}

        if 'ordered' in template:
            kwargs['ordered'] = template['ordered']

        if 'write_concern' in template:
            kwargs['write_concern'] = template['write_concern']

        return self._get_client()[database][collection].insert(
            documents, 
            **kwargs
            #callback=self._on_result,
        )


    
    def _update(self, database, template, param=None):
        collection = template['update'] if 'update' in template else None
        query = template['query'] if 'query' in template else None
        update = template['update'] if 'update' in template else None

        kwargs = {}

        if 'upsert' in template:
            kwargs['upsert'] = template['upsert']

        if 'multi' in template:
            kwargs['multi'] = template['multi']

        if 'write_concern' in template:
            kwargs['write_concern'] = template['write_concern']

        return self._get_client()[database][collection].update(
            query, 
            update,
            **kwargs
            #callback=self._on_result,
        )


    
    def _delete(self, database, template, param=None):
        collection = template['delete'] if 'delete' in template else None
        query = template['query'] if 'query' in template else None

        kwargs = {}

        if 'just_one' in template:
            kwargs['just_one'] = template['just_one']

        if 'write_concern' in template:
            kwargs['write_concern'] = template['write_concern']

        return self._get_client()[database][collection].delete(
            query, 
            **kwargs
            #callback=self._on_result,
        )


    
    def _find_and_modify(self, database, template, param=None):
        collection = template['find_and_modify'] if 'find_and_modify' in template else None
        query = template['query'] if 'query' in template else None
        sort = template['sort'] if 'sort' in template else None
        remove = template['remove'] if 'remove' in template else False
        update = template['update'] if 'update' in template else None
        new = template['new'] if 'new' in template else False
        fields = template['fields'] if 'fields' in template else None
        upsert = template['upsert'] if 'upsert' in template else False

        return self._get_client()[database][collection].find_and_modify(
            query=query, 
            sort=sort,
            remove=remove,
            update=update,
            new=new,
            fields=fields,
            upsert=upsert
            #callback=self._on_result,
        )


    def _on_result(self, result, error):
        print(result)

