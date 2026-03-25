{% macro UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}
    {{ return(adapter.dispatch('UnionByName', 'prophecy_basics')(relation_names, schemas, missingColumnOps)) }}
{% endmacro %}

{% macro default__UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}

    {# Step 1: Normalize relation list #}
    {%- if relation_names is string -%}
        {%- set relations = relation_names.split(',') | map('trim') | list -%}
    {%- else -%}
        {%- set relations = relation_names | list -%}
    {%- endif -%}

    {# Step 2: Capture column names AND build norm→actual dict per relation #}
    {%- set columns_per_relation = [] -%}
    {%- set norm_lookup_per_relation = [] -%}
    {%- for schema_blob in schemas -%}
        {%- if schema_blob is string -%}
            {%- set parsed = fromjson(schema_blob) -%}
        {%- else -%}
            {%- set parsed = schema_blob -%}
        {%- endif -%}

        {%- set col_list = [] -%}
        {%- set norm_dict = {} -%}
        {%- for f in parsed -%}
            {%- do col_list.append(f.name) -%}
            {%- do norm_dict.update({f.name | lower: f.name}) -%}
        {%- endfor -%}
        {%- do columns_per_relation.append(col_list) -%}
        {%- do norm_lookup_per_relation.append(norm_dict) -%}
    {%- endfor -%}

    {# Step 3: Normalize column names and preserve original for final SELECT #}
    {%- set norm_to_original = {} -%}
    {%- set final_columns = [] -%}
    {%- for col in columns_per_relation[0] -%}
        {%- set norm = col | lower -%}
        {%- if norm not in norm_to_original -%}
            {%- do norm_to_original.update({norm: col}) -%}
            {%- do final_columns.append(norm) -%}
        {%- endif -%}
    {%- endfor -%}

    {# Step 4: Merge schema column names #}
    {%- if missingColumnOps == 'allowMissingColumns' -%}
        {%- for other_cols in columns_per_relation[1:] -%}
            {%- for c in other_cols -%}
                {%- set norm = c | lower -%}
                {%- if norm not in norm_to_original -%}
                    {%- do norm_to_original.update({norm: c}) -%}
                    {%- do final_columns.append(norm) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- elif missingColumnOps == 'nameBasedUnionOperation' -%}
        {%- for idx in range(1, columns_per_relation | length) -%}
            {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
            {%- set extra = [] -%}
            {%- set missing = [] -%}
            {%- for c in columns_per_relation[idx] -%}
                {%- if c | lower not in norm_to_original -%}
                    {%- do extra.append(c | lower) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- for c in final_columns if c not in cur_lookup -%}
                {%- do missing.append(c) -%}
            {%- endfor -%}
            {%- if extra or missing -%}
                {{ exceptions.raise_compiler_error(
                    "Column mismatch between first relation and relation "
                    ~ (idx + 1) ~ ". Extra: " ~ extra | join(', ')
                    ~ " | Missing: " ~ missing | join(', ')
                ) }}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported missingColumnOps value: " ~ missingColumnOps) }}
    {%- endif -%}

    {# Step 5: Pre-build ALL string fragments — zero concat in the hot loop #}
    {%- set bt = "`" -%}

    {%- set null_expr = {} -%}
    {%- for norm in final_columns -%}
        {%- do null_expr.update({norm: "null as " ~ bt ~ norm_to_original[norm] ~ bt}) -%}
    {%- endfor -%}

    {%- set actual_expr_per_rel = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
        {%- set expr = {} -%}
        {%- for norm in cur_lookup -%}
            {%- if norm in norm_to_original -%}
                {%- do expr.update({norm: bt ~ cur_lookup[norm] ~ bt ~ " as " ~ bt ~ norm_to_original[norm] ~ bt}) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do actual_expr_per_rel.append(expr) -%}
    {%- endfor -%}

    {# Step 6: Assemble SELECTs — inner loop is pure lookups, zero ~ concat #}
    {%- set selects = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set rel_expr = actual_expr_per_rel[idx] -%}
        {%- set parts = [] -%}
        {%- for norm in final_columns -%}
            {%- if norm in rel_expr -%}
                {%- do parts.append(rel_expr[norm]) -%}
            {%- else -%}
                {%- do parts.append(null_expr[norm]) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do selects.append("select " ~ parts | join(', ') ~ " from " ~ relations[idx]) -%}
    {%- endfor -%}

    with union_query as (
        {{ selects | join('\nunion all\n') }}
    )
    select * from union_query

{%- endmacro %}

{% macro snowflake__UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}

    {# Step 1: Normalize relation list #}
    {%- if relation_names is string -%}
        {%- set relations = relation_names.split(',') | map('trim') | list -%}
    {%- else -%}
        {%- set relations = relation_names | list -%}
    {%- endif -%}

    {# Step 2: Capture column names AND build norm→actual dict per relation #}
    {%- set columns_per_relation = [] -%}
    {%- set norm_lookup_per_relation = [] -%}
    {%- for schema_blob in schemas -%}
        {%- if schema_blob is string -%}
            {%- set parsed = fromjson(schema_blob) -%}
        {%- else -%}
            {%- set parsed = schema_blob -%}
        {%- endif -%}

        {%- set col_list = [] -%}
        {%- set norm_dict = {} -%}
        {%- for f in parsed -%}
            {%- do col_list.append(f.name) -%}
            {%- do norm_dict.update({f.name | lower: f.name}) -%}
        {%- endfor -%}
        {%- do columns_per_relation.append(col_list) -%}
        {%- do norm_lookup_per_relation.append(norm_dict) -%}
    {%- endfor -%}

    {# Step 3: Normalize column names and preserve original for final SELECT #}
    {%- set norm_to_original = {} -%}
    {%- set final_columns = [] -%}
    {%- for col in columns_per_relation[0] -%}
        {%- set norm = col | lower -%}
        {%- if norm not in norm_to_original -%}
            {%- do norm_to_original.update({norm: col}) -%}
            {%- do final_columns.append(norm) -%}
        {%- endif -%}
    {%- endfor -%}

    {# Step 4: Merge schema column names #}
    {%- if missingColumnOps == 'allowMissingColumns' -%}
        {%- for other_cols in columns_per_relation[1:] -%}
            {%- for c in other_cols -%}
                {%- set norm = c | lower -%}
                {%- if norm not in norm_to_original -%}
                    {%- do norm_to_original.update({norm: c}) -%}
                    {%- do final_columns.append(norm) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- elif missingColumnOps == 'nameBasedUnionOperation' -%}
        {%- for idx in range(1, columns_per_relation | length) -%}
            {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
            {%- set extra = [] -%}
            {%- set missing = [] -%}
            {%- for c in columns_per_relation[idx] -%}
                {%- if c | lower not in norm_to_original -%}
                    {%- do extra.append(c | lower) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- for c in final_columns if c not in cur_lookup -%}
                {%- do missing.append(c) -%}
            {%- endfor -%}
            {%- if extra or missing -%}
                {{ exceptions.raise_compiler_error(
                    "Column mismatch between first relation and relation "
                    ~ (idx + 1) ~ ". Extra: " ~ extra | join(', ')
                    ~ " | Missing: " ~ missing | join(', ')
                ) }}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported missingColumnOps value: " ~ missingColumnOps) }}
    {%- endif -%}

    {# Step 5: Pre-build ALL string fragments — zero concat in the hot loop #}
    {%- set bt = '"' -%}

    {%- set null_expr = {} -%}
    {%- for norm in final_columns -%}
        {%- do null_expr.update({norm: "null as " ~ bt ~ norm_to_original[norm] ~ bt}) -%}
    {%- endfor -%}

    {%- set actual_expr_per_rel = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
        {%- set expr = {} -%}
        {%- for norm in cur_lookup -%}
            {%- if norm in norm_to_original -%}
                {%- do expr.update({norm: bt ~ cur_lookup[norm] ~ bt ~ " as " ~ bt ~ norm_to_original[norm] ~ bt}) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do actual_expr_per_rel.append(expr) -%}
    {%- endfor -%}

    {# Step 6: Assemble SELECTs — inner loop is pure lookups, zero ~ concat #}
    {%- set selects = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set rel_expr = actual_expr_per_rel[idx] -%}
        {%- set parts = [] -%}
        {%- for norm in final_columns -%}
            {%- if norm in rel_expr -%}
                {%- do parts.append(rel_expr[norm]) -%}
            {%- else -%}
                {%- do parts.append(null_expr[norm]) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do selects.append("select " ~ parts | join(', ') ~ " from " ~ relations[idx]) -%}
    {%- endfor -%}

    with union_query as (
        {{ selects | join('\nunion all\n') }}
    )
    select * from union_query

{%- endmacro %}

{% macro duckdb__UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}

    {# Step 1: Normalize relation list #}
    {%- if relation_names is string -%}
        {%- set relations = relation_names.split(',') | map('trim') | list -%}
    {%- else -%}
        {%- set relations = relation_names | list -%}
    {%- endif -%}

    {# Step 2: Capture column names AND build norm→actual dict per relation #}
    {%- set columns_per_relation = [] -%}
    {%- set norm_lookup_per_relation = [] -%}
    {%- for schema_blob in schemas -%}
        {%- if schema_blob is string -%}
            {%- set parsed = fromjson(schema_blob) -%}
        {%- else -%}
            {%- set parsed = schema_blob -%}
        {%- endif -%}

        {%- set col_list = [] -%}
        {%- set norm_dict = {} -%}
        {%- for f in parsed -%}
            {%- do col_list.append(f.name) -%}
            {%- do norm_dict.update({f.name | lower: f.name}) -%}
        {%- endfor -%}
        {%- do columns_per_relation.append(col_list) -%}
        {%- do norm_lookup_per_relation.append(norm_dict) -%}
    {%- endfor -%}

    {# Step 3: Normalize names #}
    {%- set norm_to_original = {} -%}
    {%- set final_columns = [] -%}
    {%- for col in columns_per_relation[0] -%}
        {%- set norm = col | lower -%}
        {%- if norm not in norm_to_original -%}
            {%- do norm_to_original.update({norm: col}) -%}
            {%- do final_columns.append(norm) -%}
        {%- endif -%}
    {%- endfor -%}

    {# Step 4: Handle missing column ops #}
    {%- if missingColumnOps == 'allowMissingColumns' -%}
        {%- for other_cols in columns_per_relation[1:] -%}
            {%- for c in other_cols -%}
                {%- set norm = c | lower -%}
                {%- if norm not in norm_to_original -%}
                    {%- do norm_to_original.update({norm: c}) -%}
                    {%- do final_columns.append(norm) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- elif missingColumnOps == 'nameBasedUnionOperation' -%}
        {%- for idx in range(1, columns_per_relation | length) -%}
            {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
            {%- set extra = [] -%}
            {%- set missing = [] -%}
            {%- for c in columns_per_relation[idx] -%}
                {%- if c | lower not in norm_to_original -%}
                    {%- do extra.append(c | lower) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- for c in final_columns if c not in cur_lookup -%}
                {%- do missing.append(c) -%}
            {%- endfor -%}
            {%- if extra or missing -%}
                {{ exceptions.raise_compiler_error(
                    "Column mismatch between first relation and relation "
                    ~ (idx + 1) ~ ". Extra: " ~ extra | join(', ')
                    ~ " | Missing: " ~ missing | join(', ')
                ) }}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported missingColumnOps value: " ~ missingColumnOps) }}
    {%- endif -%}

    {# Step 5: Pre-build ALL string fragments — zero concat in the hot loop #}
    {%- set null_expr = {} -%}
    {%- for norm in final_columns -%}
        {%- do null_expr.update({norm: "null as " ~ norm_to_original[norm]}) -%}
    {%- endfor -%}

    {%- set actual_expr_per_rel = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set cur_lookup = norm_lookup_per_relation[idx] -%}
        {%- set expr = {} -%}
        {%- for norm in cur_lookup -%}
            {%- if norm in norm_to_original -%}
                {%- do expr.update({norm: cur_lookup[norm] ~ " as " ~ norm_to_original[norm]}) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do actual_expr_per_rel.append(expr) -%}
    {%- endfor -%}

    {# Step 6: Assemble SELECTs — inner loop is pure lookups, zero ~ concat #}
    {%- set selects = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set rel_expr = actual_expr_per_rel[idx] -%}
        {%- set parts = [] -%}
        {%- for norm in final_columns -%}
            {%- if norm in rel_expr -%}
                {%- do parts.append(rel_expr[norm]) -%}
            {%- else -%}
                {%- do parts.append(null_expr[norm]) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do selects.append("select " ~ parts | join(', ') ~ " from " ~ relations[idx]) -%}
    {%- endfor -%}

    with union_query as (
        {{ selects | join('\nunion all\n') }}
    )
    select * from union_query

{%- endmacro %}
