"""This module provides routines for formula parsing and for
preprocessing formulas for analytics.py usage.
"""

from __future__ import division, unicode_literals, print_function, with_statement

import ast


def _op_str(op):
    return op.__class__.__name__


def _build_tree(ast_node):
    if type(ast_node) is ast.UnaryOp:
        return (_op_str(ast_node.op),
                _build_tree(ast_node.operand))

    if type(ast_node) is ast.BinOp:
        return ((_op_str(ast_node.op),
                _build_tree(ast_node.left),
                _build_tree(ast_node.right)))

    if type(ast_node) is ast.Name and ast_node.id in ('True', 'False'):
        return ('Bool', ast_node.id)

    if type(ast_node) is ast.Name:
        return ('Name', ast_node.id)

    if type(ast_node) is ast.Call and type(ast_node.func) is ast.Attribute and type(ast_node.func.value) is ast.Name:
        return ('MethodCall', ast_node.func.value.id, ast_node.func.attr,
                tuple([_build_tree(a) for a in ast_node.args]),
                tuple([(kw.arg, _build_tree(kw.value)) for kw in ast_node.keywords]))

    if type(ast_node) is ast.Num:
        return ('Num', ast_node.n)

    if type(ast_node) is ast.Str:
        return ('Str', ast_node.s)

    if type(ast_node) is ast.Tuple:
        return ('Tuple', tuple([_build_tree(el) for el in ast_node.elts]))

    if type(ast_node) is ast.Dict:
        return ('Dict', tuple(sorted([(_build_tree(k), _build_tree(v))
            for k, v in zip(ast_node.keys, ast_node.values)])))

    return ('Unsupported', str(ast.dump(ast_node)),)


def tree_to_expr(tree):
    if tree[0] == 'MethodCall':
        args_str = ','.join([tree_to_expr(a) for a in tree[3]])
        kwargs_str = ','.join([k + '=' + tree_to_expr(a) for k, a in tree[4]])

        if args_str:
            args_str = ',' + args_str

        if kwargs_str:
            kwargs_str = ',' + kwargs_str

        return '%s(%s%s%s)' % (
            tree[2],
            tree[1],
            args_str,
            kwargs_str
        )

    if tree[0] in ('Num', 'Str'):
        return repr(tree[1])

    if tree[0] in ('Name', 'Bool'):
        return tree[1]

    if tree[0] == 'Tuple':
        trail = ',' if len(tree[1]) == 1 else ''
        return '(%s%s)' % (
            ','.join([tree_to_expr(el) for el in tree[1]]),
            trail
        )

    if tree[0] == 'Dict':
        kv_pairs = []
        for k, v in tree[1]:
            kv_pairs.append('%s:%s' % (tree_to_expr(k), tree_to_expr(v)))
        return '{' + ','.join(kv_pairs) + '}'

    if tree[0] in ('Add', 'Sub', 'Mult', 'Div'):
        return '%s(%s,%s)' % (
            tree[0].lower(),
            tree_to_expr(tree[1]),
            tree_to_expr(tree[2])
        )
    raise ValueError('Unsupported node type ' + tree[0])

def tree_names(tree, names=None):
    if names is None:
        names = set()

    if tree[0] == 'Name':
        names.add(tree[1])
    elif tree[0] == 'Tuple':
        for i in tree[1]:
            tree_names(i, names)
    elif tree[0] in ('Add', 'Sub', 'Mult', 'Div'):
        tree_names(tree[1], names)
        tree_names(tree[2], names)
    elif tree[0] == 'Dict':
        for k, v in tree[1]:
            tree_names(v, names)
    elif tree[0] == 'MethodCall':
        obj, method_name, args, kwargs = tree[1:]
        if obj:
            names.add(obj)
        for i in args:
            tree_names(i, names)
        for k, v in kwargs:
            tree_names(v, names)


    return names

def parse(formula):
    mod = ast.parse(formula)
    assert len(mod.body) == 1
    exp = mod.body[0]
    assert type(exp) is ast.Expr

    return _build_tree(exp.value)
