from sqlalchemy import types as sqltypes, schema, util
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import OPERATORS


class MonetDDLCompiler(compiler.DDLCompiler):
    def visit_create_sequence(self, create):
        text = "CREATE SEQUENCE %s AS INTEGER" % \
               self.preparer.format_sequence(create.element)
        if create.element.start is not None:
            text += " START WITH %d" % create.element.start
        if create.element.increment is not None:
            text += " INCREMENT BY %d" % create.element.increment
        return text

    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s" % \
               self.preparer.format_sequence(drop.element)

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        impl_type = column.type.dialect_impl(self.dialect)
        if column.primary_key and \
            column is column.table._autoincrement_column and \
            not isinstance(impl_type, sqltypes.SmallInteger) and \
            (
                column.default is None or
                (
                    isinstance(column.default, schema.Sequence) and
                    column.default.optional
                )):
            colspec += " INT AUTO_INCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_check_constraint(self, constraint):
        util.warn("Skipped unsupported check constraint %s" % constraint.name)


class MonetTypeCompiler(compiler.GenericTypeCompiler):
    def visit_DOUBLE_PRECISION(self, type_):
        return "DOUBLE PRECISION"

    def visit_INET(self, type_):
        return "INET"

    def visit_URL(self, type_):
        return "URL"

    def visit_WRD(self, type_):
        return "WRD"

    def visit_datetime(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_TIMESTAMP(self, type_):
        if type_.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    def visit_VARCHAR(self, type_):
        if type_.length is None:
            return "CLOB"
        return compiler.GenericTypeCompiler.visit_VARCHAR(self, type_)


class MonetCompiler(compiler.SQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)

    def visit_sequence(self, seq):
        exc = "(SELECT NEXT VALUE FOR %s)" \
              % self.dialect.identifier_preparer.format_sequence(seq)
        return exc

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text += "\nLIMIT " + str(select._limit)
        if select._offset is not None:
            text += " OFFSET " + str(select._offset)
        return text

    def visit_extended_join(self, join, asfrom=False, **kwargs):
        """Support for full outer join, created by
        rb.data.sqlalchemy.ExtendedJoin
        """

        if join.isouter and join.isfullouter:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "

        return (
            join.left._compiler_dispatch(self, asfrom=True, **kwargs) +
            join_type +
            join.right._compiler_dispatch(self, asfrom=True, **kwargs) +
            " ON " +
            join.onclause._compiler_dispatch(self, **kwargs)
        )

    def visit_ne(self, element, **kwargs):
        return (
            element.left._compiler_dispatch(self, **kwargs) +
            " <> " +
            element.right._compiler_dispatch(self, **kwargs))

    def visit_compound_select(self, cs, asfrom=False,
                              parens=True, compound_index=0, **kwargs):
        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]
        need_result_map = toplevel or \
                          (compound_index == 0
                           and entry.get('need_result_map_for_compound', False))

        self.stack.append(
            {
                'correlate_froms': entry['correlate_froms'],
                'asfrom_froms': entry['asfrom_froms'],
                'selectable': cs,
                'need_result_map_for_compound': need_result_map
            })

        keyword = self.compound_keywords.get(cs.keyword)

        text = (" " + keyword + " ").join(
            (c._compiler_dispatch(self,
                                  asfrom=asfrom, parens=False,
                                  compound_index=i, **kwargs)
             for i, c in enumerate(cs.selects))
        )

        group_by = cs._group_by_clause._compiler_dispatch(
            self, asfrom=asfrom, with_in_group=True, **kwargs)
        if group_by:
            text += " GROUP BY " + group_by

        text += self.order_by_clause(cs, **kwargs)
        text += (cs._limit_clause is not None
                 or cs._offset_clause is not None) and \
                self.limit_clause(cs, **kwargs) or ""

        if self.ctes and toplevel:
            text = self._render_cte_clause() + text

        self.stack.pop(-1)
        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def visit_select(self, select, asfrom=False, parens=True,
                     fromhints=None,
                     compound_index=0,
                     nested_join_translation=False,
                     select_wraps_for=None,
                     **kwargs):

        needs_nested_translation = \
            select.use_labels and \
            not nested_join_translation and \
            not self.stack and \
            not self.dialect.supports_right_nested_joins

        if needs_nested_translation:
            transformed_select = self._transform_select_for_nested_joins(
                select)
            text = self.visit_select(
                transformed_select, asfrom=asfrom, parens=parens,
                fromhints=fromhints,
                compound_index=compound_index,
                nested_join_translation=True, **kwargs
            )

        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = toplevel or \
                              (
                                  compound_index == 0 and entry.get(
                                      'need_result_map_for_compound', False)
                              ) or entry.get('need_result_map_for_nested', False)

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and 'add_to_result_map' in kwargs:
            del kwargs['add_to_result_map']

        if needs_nested_translation:
            if populate_result_map:
                self._transform_result_map_for_nested_joins(
                    select, transformed_select)
            return text

        froms = self._setup_select_stack(select, entry, asfrom)

        column_clause_args = kwargs.copy()
        column_clause_args.update({
            'within_label_clause': False,
            'within_columns_clause': False
        })

        text = "SELECT "  # we're off to a good start !

        if select._hints:
            hint_text, byfrom = self._setup_select_hints(select)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select._prefixes:
            text += self._generate_prefixes(
                select, select._prefixes, **kwargs)

        text += self.get_select_precolumns(select, **kwargs)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c for c in [
                self._label_select_column(
                    select,
                    column,
                    populate_result_map, asfrom,
                    column_clause_args,
                    name=name)
                for name, column in select._columns_plus_names
                ]
            if c is not None
            ]

        if populate_result_map and select_wraps_for is not None:
            # if this select is a compiler-generated wrapper,
            # rewrite the targeted columns in the result map
            wrapped_inner_columns = set(select_wraps_for.inner_columns)
            translate = dict(
                (outer, inner.pop()) for outer, inner in [
                    (
                        outer,
                        outer.proxy_set.intersection(wrapped_inner_columns))
                    for outer in select.inner_columns
                    ] if inner
            )
            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
                ]

        text = self._compose_select_body(
            text, select, inner_columns, froms, byfrom, kwargs)

        if select._statement_hints:
            per_dialect = [
                ht for (dialect_name, ht)
                in select._statement_hints
                if dialect_name in ('*', self.dialect.name)
                ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        if self.ctes and self._is_toplevel_select(select):
            text = self._render_cte_clause() + text

        if select._suffixes:
            text += " " + self._generate_prefixes(
                select, select._suffixes, **kwargs)

        self.stack.pop(-1)

        if asfrom and parens:
            return "(" + text + ")"
        else:
            return text

    def _compose_select_body(
            self, text, select, inner_columns, froms, byfrom, kwargs):
        text += ', '.join(inner_columns)

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ', '.join(
                    [f._compiler_dispatch(self, asfrom=True,
                                          fromhints=byfrom, **kwargs)
                     for f in froms])
            else:
                text += ', '.join(
                    [f._compiler_dispatch(self, asfrom=True, **kwargs)
                     for f in froms])
        else:
            text += self.default_from()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nWHERE " + t

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(
                self, with_in_group=True, **kwargs)
            if group_by:
                text += " GROUP BY " + group_by

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nHAVING " + t

        if select._order_by_clause.clauses:
            text += self.order_by_clause(select, **kwargs)

        if (select._limit_clause is not None or
                    select._offset_clause is not None):
            text += self.limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            text += self.for_update_clause(select, **kwargs)

        return text

    def visit_clauselist(self, clauselist, with_in_group=False, **kw):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        else:
            sep = OPERATORS[clauselist.operator]

        return sep.join(
            s for s in
            (
                c._compiler_dispatch(self, render_label_as_label=(c if with_in_group else None), **kw)
                for c in clauselist.clauses)
            if s)
