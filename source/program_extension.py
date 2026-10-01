"""Generic multi-output reversible-program codec. No dataset-specific logic.

Functions and typed output schemas are supplied by an offline proposal file.
The decoder reads them from the archive. Failures use byte-exact exceptions.
"""
import copy
import hashlib
import json
import shutil
import tempfile
from pathlib import Path


def install(c):
    prior_read = c.read_tag_stream

    def column_write(directory, name, values, kind):
        if len(set(values)) == 1:
            return {'constant': values[0], 'count': len(values)}
        if kind == 'int':
            entry = c.r44_int_write(directory, name, values, 'delta')
            return {'numeric': entry, 'count': len(values)}
        spec = c.ExtractSpec(tag=name, pattern='', kind=c.fixed_string_factor_kind(values), store_group=0)
        return {'stream': c.write_tag_stream(directory, spec, values), 'count': len(values)}

    def column_read(directory, entry):
        if 'constant' in entry:
            return [entry['constant']] * entry['count']
        if 'numeric' in entry:
            return c.r44_int_read(directory, entry['numeric'])
        return c.read_tag_stream(directory, entry['stream'])

    def read(directory, entry):
        if entry.get('kind') != 'llm_multi_program':
            return prior_read(directory, entry)
        program = entry['program']
        _, inverse = c.compile_generated_python_exec(program['code'])
        columns = [column_read(directory, x) for x in entry['columns']]
        layouts = [column_read(directory, x) for x in entry['layouts']]
        n = entry['selected_count']
        if any(len(x) != n for x in columns + layouts):
            raise ValueError('Program column count mismatch')
        selected = []
        for i in range(n):
            record = {'stored': [x[i] for x in columns], 'layout': [x[i] for x in layouts]}
            value = inverse(record)
            if not isinstance(value, str):
                raise ValueError('Program inverse must return str')
            selected.append(value)
        if 'route_file' in entry:
            flags = (directory / entry['route_file']).read_bytes()
            exceptions = c.base.read_string_stream(directory / entry['exception_file'])
            result, a, b = [], 0, 0
            for flag in flags:
                if flag == 1:
                    result.append(selected[a]); a += 1
                elif flag == 0:
                    result.append(exceptions[b]); b += 1
                else:
                    raise ValueError('Invalid route flag')
            if a != len(selected) or b != len(exceptions):
                raise ValueError('Unused program values')
        else:
            result = selected
        if len(result) != entry['count']:
            raise ValueError('Program stream count mismatch')
        return result

    def run_record(value, program, forward, inverse):
        try:
            record = forward([value])
            if not isinstance(record, dict):
                return None
            fields, layout = record.get('stored'), record.get('layout')
            if not isinstance(fields, list) or not isinstance(layout, list):
                return None
            if len(fields) != len(program['types']) or len(layout) != program['layout_arity']:
                return None
            for item, kind in zip(fields, program['types']):
                if kind == 'int':
                    if type(item) is not int or not -(2**62) < item < 2**62:
                        return None
                elif kind == 'str':
                    if not isinstance(item, str):
                        return None
                    item.encode('latin-1')
                else:
                    return None
            if any(type(x) is not int or x < 0 or x > 255 for x in layout):
                return None
            if inverse(copy.deepcopy(record)) != value:
                return None
            return record
        except (ValueError, TypeError, IndexError, KeyError, OverflowError, ZeroDivisionError):
            return None

    def write(directory, name, values, program):
        if not values:
            return None
        forward, inverse = c.compile_generated_python_exec(program['code'])
        # This memo exists only for this stream in this block.
        memo = {v: run_record(v, program, forward, inverse) for v in dict.fromkeys(values)}
        records = [memo[v] for v in values if memo[v] is not None]
        if not records:
            return None
        columns = [column_write(directory, name + '_c' + str(i),
                                [r['stored'][i] for r in records], kind)
                   for i, kind in enumerate(program['types'])]
        layouts = [column_write(directory, name + '_w' + str(i),
                                [r['layout'][i] for r in records], 'int')
                   for i in range(program['layout_arity'])]
        entry = {'kind': 'llm_multi_program', 'tag': name, 'count': len(values),
                 'selected_count': len(records), 'columns': columns, 'layouts': layouts,
                 'program': {k: program[k] for k in ('code', 'types', 'layout_arity')}}
        if len(records) != len(values):
            route, exception = name + '.route.bin', name + '.exception.bin'
            (directory / route).write_bytes(bytes(int(memo[v] is not None) for v in values))
            c.base.write_string_stream(directory / exception, [v for v in values if memo[v] is None])
            entry.update(route_file=route, exception_file=exception)
        if read(directory, entry) != values:
            raise ValueError('Program codec roundtrip mismatch')
        return entry

    def targets(metadata):
        # Whole streams or dictionary pools, never columns produced by ourselves.
        found = {}
        for outer in metadata['streams']:
            leaf = outer.get('python_exec_string_codec', outer)
            if leaf.get('kind') == 'r44_dictionary':
                if 'pool_stream' in leaf:
                    key = json.dumps(leaf['pool_stream'], sort_keys=True)
                    found.setdefault(key, []).append((leaf, 'pool_stream'))
                else:
                    key = 'file:' + leaf['pool']
                    found.setdefault(key, []).append((leaf, 'pool'))
            elif leaf.get('kind') in {'string', 'string_mtf_rank', 'string_mtf_digit_literals', 'open_function', 'affixed_int_delta'}:
                found['leaf:' + str(leaf['tag'])] = [(leaf, None)]
        return list(found.values())

    def transform(directory, metadata, program):
        stats = []
        for i, references in enumerate(targets(metadata)):
            leaf, key = references[0]
            if 'target_tags' in program and not any(x[0].get('tag') in program['target_tags'] for x in references):
                continue
            if key == 'pool':
                values = c.base.read_string_stream(directory / leaf[key])
            else:
                values = c.read_tag_stream(directory, leaf[key] if key else leaf)
            name = 'r53_' + hashlib.sha256(program['code'].encode()).hexdigest()[:12] + '_' + str(i)
            entry = write(directory, name, values, program)
            if entry is None:
                continue
            for target, field in references:
                if field == 'pool':
                    target.pop('pool')
                    target['pool_stream'] = copy.deepcopy(entry)
                elif field:
                    target[field] = copy.deepcopy(entry)
                else:
                    kept = {k: target[k] for k in ('tag', 'placeholder') if k in target}
                    target.clear()
                    target.update(copy.deepcopy(entry), **kept)
            stats.append({'target': i, 'values': len(values), 'matched': entry['selected_count']})
        c.r47_cleanup(directory, metadata)
        c.deduplicate_block_files(directory, metadata)
        return stats

    c.read_tag_stream = read
    c.r53_write = write
    c.r53_transform_program = transform
    c.r53_run_record = run_record
