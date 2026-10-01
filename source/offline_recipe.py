"""Replay an offline-selected storage recipe; never load training data online.

This is an experimental speed/size trade-off. It does NOT preserve R52's
per-block optimality guarantee because the online candidate search is removed.
"""
import json
import os
from pathlib import Path


def apply(c, directory, metadata, recipe):
    c.R47_STATS.clear()
    c.R49_STATS.clear()
    representation = recipe['generic_representation']
    if representation == 'file_alias':
        c.deduplicate_block_files(directory, metadata)
    elif representation in ('whole_dictionary', 'split_dictionary'):
        c.r49_dictionary(directory, metadata, representation == 'split_dictionary')
    else:
        raise ValueError('Unknown offline representation')
    c.R47_STATS.append({'stage': 'generic_representation', 'selected': representation, 'offline': True})
    if any(o.get('kind') == 'r44_dictionary' for o in c.r47_walk(metadata)):
        mode = recipe.get('numeric', 'unchanged')
        if mode != 'unchanged':
            c.r47_numeric_recode(directory, metadata, mode)
        c.R47_STATS.append({'stage': 'numeric', 'selected': mode, 'offline': True})
    if any(o.get('kind') == 'r44_columns' for o in c.r47_walk(metadata)):
        mode = recipe.get('subfield_sharing', 'unchanged')
        if mode != 'unchanged':
            c.r47_share_subfields(directory, metadata, mode)
        c.R47_STATS.append({'stage': 'subfield_sharing', 'selected': mode, 'offline': True})


def install(c):
    parent = c.r49_transform

    def transform(directory, metadata, profile):
        path = os.environ.get('SEMZIP_R53_OFFLINE_RECIPE')
        if path and profile == 'adaptive':
            return apply(c, directory, metadata, json.loads(Path(path).read_text())['recipe'])
        return parent(directory, metadata, profile)

    c.r49_transform = transform
