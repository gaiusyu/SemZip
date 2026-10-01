"""Lower learned numeric displays without changing their executable program."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import sys

try:
    from re import _parser as regex_parser
except ImportError:
    import sre_parse as regex_parser


def supported_structure(sequence, inside_repeat=False):
    for opcode, argument in sequence:
        name = str(opcode)
        if name in {'MAX_REPEAT', 'MIN_REPEAT'}:
            if inside_repeat or not supported_structure(argument[2], True):
                return False
        elif name == 'SUBPATTERN':
            if not supported_structure(argument[-1], inside_repeat):
                return False
        elif name == 'BRANCH':
            if any(not supported_structure(branch, inside_repeat) for branch in argument[1]):
                return False
        elif name not in {'LITERAL', 'NOT_LITERAL', 'IN', 'CATEGORY', 'ANY', 'AT'}:
            return False
    return True


def broaden_display_pattern(pattern):
    original = re.compile(pattern)
    # Verbose mode changes the meaning of spaces/comments; leave it untouched.
    if original.flags & re.VERBOSE or re.search(r'\(\?[aiLmsux-]*x[aiLmsux-]*[:)]', pattern):
        return pattern, 'verbose-mode-unchanged'
    if not supported_structure(regex_parser.parse(pattern)):
        return pattern, 'unsupported-regex-structure'
    out = []
    index = 0
    while index < len(pattern):
        start = index
        if pattern.startswith('(?#', index):
            index += 3
            while index < len(pattern):
                if pattern[index] == '\\':
                    index += 2
                elif pattern[index] == ')':
                    index += 1
                    break
                else:
                    index += 1
            out.append(pattern[start:index])
            continue
        char = pattern[index]
        if char == '[':
            index += 1
            if index < len(pattern) and pattern[index] == '^':
                index += 1
            if index < len(pattern) and pattern[index] == ']':
                index += 1
            while index < len(pattern):
                if pattern[index] == '\\':
                    index += 2
                elif pattern[index] == ']':
                    index += 1
                    break
                else:
                    index += 1
            atom = pattern[start:index]
        elif char == '\\':
            index += 2
            atom = pattern[start:index]
        elif char == ' ':
            while index < len(pattern) and pattern[index] == ' ':
                index += 1
            quantified = index < len(pattern) and pattern[index] in '+*?{'
            out.append(pattern[start:index] if quantified else '[ ]+')
            continue
        else:
            out.append(char)
            index += 1
            continue
        bound = re.match(r'\{([1-9]\d*)(?:,(\d+))?\}', pattern[index:])
        if atom in {r'\d', '[0-9]'} and bound:
            out.append(atom + '+')
            index += len(bound.group(0))
        else:
            out.append(atom)
    result = ''.join(out)
    try:
        changed = re.compile(result)
        if (changed.groups, changed.groupindex, changed.flags) != (
                original.groups, original.groupindex, original.flags):
            return pattern, 'capture-or-flags-changed'
        if not supported_structure(regex_parser.parse(result)):
            return pattern, 'new-nested-repeat-rejected'
    except re.error:
        return pattern, 'rewritten-pattern-invalid'
    return result, 'broadened' if result != pattern else 'unchanged'


def lower_item(item):
    result = copy.deepcopy(item)
    program = result.get('program', {})
    if (item.get('semantic_class') not in {'class_1_composed_numeric', 'class_2_formatted_numeric'}
            or item.get('kind') != 'open_function' or program.get('op') != 'python_exec'
            or item.get('store_group') != 0):
        return result, {'status': 'not-whole-span-numeric-program'}
    replacement = item.get('replacement', '')
    if replacement.count('{placeholder}') != 1:
        return result, {'status': 'not-single-placeholder'}
    prefix, suffix = replacement.split('{placeholder}')
    if any(char in prefix + suffix for char in '{}'):
        return result, {'status': 'nonliteral-replacement-context'}
    pattern, pattern_status = broaden_display_pattern(item['pattern'])
    group_regex = program.get('group_regex', '')
    groups, group_status = broaden_display_pattern(group_regex) if group_regex else ('', 'absent')
    if pattern == item['pattern'] and groups == group_regex:
        return result, {'status': 'unchanged', 'pattern_status': pattern_status,
                        'group_status': group_status}
    result['pattern'] = pattern
    program['group_regex'] = groups
    program['_layout_polymorphic_shared_delta'] = True
    program['_layout_placeholder_prefix'] = prefix
    program['_layout_placeholder_suffix'] = suffix
    assert program['code'] == item['program']['code']
    return result, {'status': 'lowered', 'old_pattern': item['pattern'],
                    'new_pattern': pattern, 'pattern_status': pattern_status,
                    'group_status': group_status}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--audit', type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    plan = json.loads(raw)
    records = []
    for index, item in enumerate(plan['specs']):
        plan['specs'][index], record = lower_item(item)
        records.append({'tag': item['tag'], **record})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, sort_keys=True, indent=2))
    args.audit.write_text(json.dumps({'stage': 'offline syntax-aware display lowering',
        'source_sha256': hashlib.sha256(raw).hexdigest(),
        'output_sha256': hashlib.sha256(args.output.read_bytes()).hexdigest(),
        'lowering_source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'records': records}, indent=2))


if __name__ == '__main__':
    main()
