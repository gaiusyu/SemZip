"""Offline-selected recipes and programs; online does not search candidates."""
import json
import os
from pathlib import Path


def apply(c, root, meta, plan):
    directory=root/'dataset_extract'
    recipe=plan['recipe']
    if recipe.get('numeric') == 'adaptive':
        raise ValueError('Online adaptive mode is prohibited; freeze individual modes offline')
    c._R54_PLAN=plan
    c._R54_ONLINE=True
    try:
        import offline_recipe
        offline_recipe.apply(c,directory,meta,recipe)
        for program in plan['programs']:
            stats=c.r53_transform_program(directory,meta,program)
            c.R47_STATS.append({'stage':'llm_program','selected':program['id'],
                               'offline':True,'targets':stats})
    finally:
        c._R54_ONLINE=False
        c._R54_PLAN={}


def install(c):
    c._R54_ONLINE=False
    c._R54_PLAN={}
    parent=c.save_extract_streams
    choose=c.r47_choose
    def guard(*a,**kw):
        if c._R54_ONLINE:
            raise RuntimeError('Online candidate selection was unexpectedly called')
        return choose(*a,**kw)
    c.r47_choose=guard
    def save(root,meta,values,text):
        path=os.environ.get('SEMZIP_R54_PLAN')
        if not path:
            return parent(root,meta,values,text)
        c._r52_parent_save(root,meta,values,text)
        apply(c,root,meta,json.loads(Path(path).read_text()))
    c.save_extract_streams=save
    c.r54_apply=lambda root,meta,plan: apply(c,root,meta,plan)
