import unittest
import formula_parser
from pprint import pprint

class FormulaParserTestCase(unittest.TestCase):
    def test_formula_parser_simple_arithmetic(self):
        assert  ('Add', ('Name', 'A'), ('Name', 'B')) == formula_parser.parse('A+B')
        assert  ('Mult', ('Name', 'A'), ('Name', 'B')) == formula_parser.parse('A*B')
        assert  ('Div', ('Name', 'A'), ('Name', 'B')) == formula_parser.parse('A/B')
        assert  ('Sub', ('Name', 'A'), ('Name', 'B')) == formula_parser.parse('A-B')

    def test_formula_parser_simple_arithmetic_to_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert 'add(A,B)' == to_e('A+B')
        assert 'mult(A,B)' == to_e('A*B')
        assert 'div(A,B)' == to_e('A/B')
        assert 'sub(A,B)' == to_e('A-B')


    def test_formula_parser_nested_parens(self):
        assert formula_parser.parse('A*B/(A+B)') == (
            'Div',
             ('Mult', ('Name', 'A'), ('Name', 'B')),
             ('Add', ('Name', 'A'), ('Name', 'B'))
        )

    def test_formula_parser_nested_parens_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert to_e('A*B/(A+B)') == 'div(mult(A,B),add(A,B))'

    def test_formula_parser_basic_method_call(self):
        assert ('MethodCall', 'A', 'prev_period', (), ()) == formula_parser.parse('A.prev_period()')

    def test_formula_parser_basic_method_call_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert 'prev_period(A)' == to_e('A.prev_period()')



    def test_formula_parser_method_call_with_args(self):
        assert formula_parser.parse('A.some_method("string", B, 1, 2.3, (1, 2, 3))') == (
            'MethodCall',
                 'A',
                 'some_method',
                 (
                    ('Str', 'string'),
                    ('Name', 'B'),
                    ('Num', 1),
                    ('Num', 2.3),
                    ('Tuple', (('Num', 1), ('Num', 2), ('Num', 3)))
                ),
                ()
        )

    def test_formula_parser_method_call_with_args_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert to_e('A.some_method("string", B, 1, 2.3, (1, 2, 3))') == \
                "some_method(A,'string',B,1,2.3,(1,2,3))"

    def test_formula_parser_method_call_with_args_and_kwargs(self):
        assert formula_parser.parse('A.some_method("string", some_setting=True)') == (
            'MethodCall',
             'A',
             'some_method',
             (('Str', 'string'),),
             (('some_setting', ('Bool', 'True')),))

        assert formula_parser.parse('A.some_method(some_setting=True)') == (
            'MethodCall',
             'A',
             'some_method',
             (),
             (('some_setting', ('Bool', 'True')),))

    def test_formula_parser_method_call_with_args_and_kwargs_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert to_e('A.some_method("string", some_setting=True)') == \
                "some_method(A,'string',some_setting=True)"

        assert to_e('A.some_method(some_setting=True)') == \
                "some_method(A,some_setting=True)"

    def test_formula_parser_method_call_with_dict(self):

        assert formula_parser.parse('A.some_method({\'some dimension\':\'XYZ\'})') == (
            'MethodCall',
            'A',
            'some_method',
            (('Dict', ((('Str', 'some dimension'), ('Str', 'XYZ')),)),),
            ()
        )

    def test_formula_parser_method_call_with_dict_expr(self):
        to_e = lambda f:formula_parser.tree_to_expr(formula_parser.parse(f))
        assert to_e('A.some_method({\'some dimension\':\'XYZ\'})') == \
                "some_method(A,{'some dimension':'XYZ'})"

    def test_tree_names_for_simple_arithmetic(self):
        tree = formula_parser.parse('(A+B)/(C-D)*X')
        self.assertEqual(
            'ABCDX',
            ''.join(sorted(list(formula_parser.tree_names(tree)))))

    def test_tree_names_for_method_call_args_and_kwargs(self):
        tree = formula_parser.parse('A.some_method(X, Y, some_arg=Z)/A')
        assert 'AXYZ' == ''.join(sorted(list(formula_parser.tree_names(tree))))

    def test_tree_names_for_method_call_with_tuple(self):
        tree = formula_parser.parse('A.some_method((X, Y, 7))/A')
        assert 'AXY' == ''.join(sorted(list(formula_parser.tree_names(tree))))

    def test_tree_names_for_method_call_with_dict(self):
        tree = formula_parser.parse('A.some_method({"X":X, "Y":7})/A')
        assert 'AX' == ''.join(sorted(list(formula_parser.tree_names(tree))))



if __name__ == '__main__':
    unittest.main()
