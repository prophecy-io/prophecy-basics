{#
  XMLParse Macro Gem
  ==================

  Reads XML stored in a text column and expands it into a structured column you
  can query, using a schema or a sample document—or leaves the table unchanged
  when parsing is disabled.

  Parameters:
    - relation_name (list): Source relation(s).
    - columnName: Column to parse; empty → SELECT *.
    - parsingMethod: 'parseFromSchema' | 'parseFromSampleRecord' | 'parseToColumns' | 'none' / empty.
    - sampleRecord, sampleSchema: XML sample or schema string for Spark from_xml.
    - parsingMethod 'parseToColumns' (Snowflake, Databricks/Spark): one string column per entry of
      outputColumns, with the three arguments below. Any other method keeps the legacy output.
    - outputColumns: list of {name, kind, tag?, instance?, attr?} (Alteryx XML Parse).
    - includeSourceColumn: keep the source XML column.
    - ignoreErrors: malformed XML gives NULLs instead of failing.
    - childElement: parse every outermost element with this tag, at any depth, one output row per
      occurrence (input rows with none are dropped); the root* kinds then refer to that element.
      Empty → parse the document's root element. On Databricks the tag must not nest inside itself
      (the query fails rather than guess).

  Adapter Support:
    - default__ (from_xml; parseToColumns: xpath_* + regex), snowflake__ (PARSE_XML; parseToColumns:
      XMLGET + FLATTEN), duckdb__ (pass-through SELECT *)

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.XMLParse(['t'], 'xml_col', 'parseFromSchema', '', '<root>...</root>') }}
    {{ prophecy_basics.XMLParse(['t'], 'xml_col', 'none', '', '') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.XMLParse(['t'], 'xml_col', 'parseFromSchema', '', '<root>...</root>') }}

    Resolved query (default__):
      select
          *,
          from_xml(`xml_col`, '<root>...</root>') as `xml_col_parsed`
      from t
#}
{% macro XMLParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema,
    outputColumns=[],
    includeSourceColumn=true,
    ignoreErrors=true,
    childElement='') -%}
    {{ return(adapter.dispatch('XMLParse', 'prophecy_basics')(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema,
    outputColumns,
    includeSourceColumn,
    ignoreErrors,
    childElement)) }}
{% endmacro %}


{% macro default__XMLParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema,
    outputColumns=[],
    includeSourceColumn=true,
    ignoreErrors=true,
    childElement=''
) %}

    {{ log("Parsing XML using method: " ~ parsingMethod, info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseToColumns' and not outputColumns -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseToColumns' -%}
        {#-
          Alteryx-style XML Parse on Databricks / Spark: the same outputColumns, includeSourceColumn,
          ignoreErrors and childElement as on Snowflake, enabled by parsingMethod='parseToColumns' (a
          Databricks gem also carries generated outputColumns; they only apply in this mode).
          Values come from the xpath_* functions (path literals, one parse per call), trimmed like
          Snowflake's. XPath cannot print a node, so outer XML is cut out of the document with a regex,
          which is only trusted when XPath confirms it found exactly the right elements; otherwise the
          query fails with raise_error rather than return a wrong value. The same holds for
          childElement, which also needs <tag> not to nest inside another <tag> on Databricks.
          ignoreErrors: from_xml's corrupt-record column flags malformed XML before any xpath runs.
        -#}
        {%- set src = "`" ~ (columnName | replace("`", "``")) ~ "`" -%}
        {%- set el = "`__prophecy_el`" -%}
        {%- set doc = el if childElement else src -%}
        {%- set valid -%}
            {{ src }} IS NOT NULL AND from_xml({{ src }}, '_c STRING', map('columnNameOfCorruptRecord', '_c'))._c IS NULL
        {%- endset -%}
        {%- set ok = 'true' if (childElement or not ignoreErrors) else valid -%}
        {%- set excluded = ([el] if childElement else []) + ([] if includeSourceColumn else [src]) -%}
        select
            {{ '* except (' ~ (excluded | join(', ')) ~ ')' if excluded else '*' }}
            {%- for c in outputColumns %}
            {%- set kind = c['kind'] -%}
            {%- set node = '/*' if kind.startswith('root') else '/*/' ~ c['tag'] ~ '[' ~ ((c.get('instance', 0) | int) + 1) ~ ']' -%}
            {%- set attr_path = node ~ '/@' ~ c.get('attr', '') -%}
            ,
            {%- if kind in ('root', 'child') %}
            CASE WHEN {{ ok }} AND xpath_boolean({{ doc }}, 'boolean({{ node }})')
                 THEN regexp_replace(xpath_string({{ doc }}, '{{ node }}[not(*)]'), '^\\s+|\\s+$', '') END
            {%- elif kind in ('root_attr', 'child_attr') %}
            CASE WHEN {{ ok }} AND xpath_boolean({{ doc }}, 'boolean({{ attr_path }})')
                 THEN xpath_string({{ doc }}, '{{ attr_path }}') END
            {%- elif kind == 'root_outer' %}
            CAST({{ doc }} AS STRING)
            {%- elif kind == 'child_outer' %}
            {%- set tag = c['tag'] -%}
            {%- set pattern = "(?s)<" ~ (tag | replace(".", "\\\\.")) ~ "(?=[\\\\s/>])(?:[^>\"\\']|\"[^\"]*\"|\\'[^\\']*\\')*?(?:/>|>.*?</" ~ (tag | replace(".", "\\\\.")) ~ "\\\\s*>)" -%}
            CASE WHEN {{ ok }} AND xpath_boolean({{ doc }}, 'boolean({{ node }})') THEN
                CASE WHEN NOT xpath_boolean({{ doc }}, 'boolean(/{{ tag }} | /*/*//{{ tag }})')
                      AND xpath_int({{ doc }}, 'count(/*/{{ tag }})') = size(regexp_extract_all({{ doc }}, '{{ pattern }}', 0))
                     THEN try_element_at(regexp_extract_all({{ doc }}, '{{ pattern }}', 0), {{ (c.get('instance', 0) | int) + 1 }})
                     ELSE raise_error('XMLParse: cannot cut out the outer XML of <{{ tag }}> on Databricks: it also appears below another element, as the root, or in a comment or CDATA')
                END
            END
            {%- else -%}
            {{ exceptions.raise_compiler_error("XMLParse: unknown output column kind '" ~ kind ~ "'") }}
            {%- endif %} as `{{ c['name'] | replace("`", "``") }}`
            {%- endfor %}
        {%- if childElement %}
        {%- set tag = childElement -%}
        {%- set pattern = "(?s)<" ~ (tag | replace(".", "\\\\.")) ~ "(?=[\\\\s/>])(?:[^>\"\\']|\"[^\"]*\"|\\'[^\\']*\\')*?(?:/>|>.*?</" ~ (tag | replace(".", "\\\\.")) ~ "\\\\s*>)" -%}
        from (
            select *
            from {{ relation_list | join(', ') }}
            lateral view explode(
                CASE WHEN {{ valid if ignoreErrors else src ~ ' IS NOT NULL' }} THEN
                    CASE WHEN xpath_int({{ src }}, 'count(/*//{{ tag }})') = 0 THEN array()
                         WHEN NOT xpath_boolean({{ src }}, 'boolean(/{{ tag }} | /*//{{ tag }}//{{ tag }})')
                              AND xpath_int({{ src }}, 'count(/*//{{ tag }})') = size(regexp_extract_all({{ src }}, '{{ pattern }}', 0))
                         THEN regexp_extract_all({{ src }}, '{{ pattern }}', 0)
                         ELSE raise_error('XMLParse: cannot isolate each <{{ tag }}> on Databricks: <{{ tag }}> nests inside another <{{ tag }}>, is the root, or appears in a comment or CDATA')
                    END
                END
            ) __prophecy_matches as {{ el }}
        )
        {%- else %}
        from {{ relation_list | join(', ') }}
        {%- endif %}

    {%- else -%}
        {%- set quoted_col = "`" ~ columnName ~ "`" -%}
        {#- Spark SQL string literals escape with a backslash: escape backslashes first, then quotes -#}
        {%- set schema_literal = sampleSchema | replace("\n", " ") | replace("\\", "\\\\") | replace("'", "\\'") -%}
        {%- set record_literal = sampleRecord | replace("\n", " ") | replace("\\", "\\\\") | replace("'", "\\'") -%}
        {%- set alias_col = "`" ~ columnName ~ "_parsed`" -%}

        {%- if parsingMethod == 'parseFromSchema' -%}
            select
                *,
                from_xml({{ quoted_col }}, '{{ schema_literal }}') as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                from_xml({{ quoted_col }}, schema_of_xml('{{ record_literal }}')) as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'none' or not parsingMethod -%}
            select * from {{ relation_list | join(', ') }}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid parsingMethod: '" ~ parsingMethod ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
            ) }}
        {%- endif -%}
    {%- endif -%}

{% endmacro %}

{% macro snowflake__XMLParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema,
    outputColumns=[],
    includeSourceColumn=true,
    ignoreErrors=true,
    childElement=''
) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod != 'parseToColumns' or not outputColumns -%}
        {#- Legacy behaviour: one VARIANT column holding the parsed document. -#}
        select
            *,
            PARSE_XML({{ '"' ~ columnName ~ '"' }}) as {{ '"' ~ columnName ~ '_parsed"' }}
        from {{ relation_list | join(', ') }}

    {%- else -%}
        {#-
          Alteryx-style XML Parse: one output column per entry of outputColumns.
          Each entry is a dict:
            name     - output column name
            kind     - root | root_outer | root_attr | child | child_outer | child_attr
            tag      - child element name (child*)
            instance - 0-based occurrence of tag among the root's children (child*; default 0)
            attr     - attribute name (*_attr)
          With childElement, root/root_outer/root_attr are the matched element, not the document root.
          Values are strings exactly as written (PARSE_XML's disable_auto_convert = TRUE, so 1.50 stays
          1.50); a child with element children (or mixed content) yields ''.
        -#}
        {%- set src = '"' ~ (columnName | replace('"', '""')) ~ '"' -%}
        {%- set doc = '"__prophecy_xml"' -%}
        {%- set parsed -%}
            {%- if ignoreErrors -%}
                CASE WHEN CHECK_XML({{ src }}) IS NULL THEN PARSE_XML({{ src }}, TRUE) END
            {%- else -%}
                PARSE_XML({{ src }}, TRUE)
            {%- endif -%}
        {%- endset -%}
        {%- set el = '"__prophecy_el"' if childElement else doc -%}
        {%- set helpers = [doc, el, '"__prophecy_seq"', '"__prophecy_path"'] if childElement else [doc] -%}
        {%- set excluded = helpers if includeSourceColumn else helpers + [src] -%}
        select
            * exclude ({{ excluded | join(', ') }})
            {%- for c in outputColumns %}
            {%- set kind = c['kind'] -%}
            {%- set node -%}
                {%- if kind.startswith('child') -%}
                    XMLGET({{ el }}, '{{ c['tag'] | replace("'", "''") }}', {{ c.get('instance', 0) | int }})
                {%- else -%}
                    {{ el }}
                {%- endif -%}
            {%- endset -%}
            ,
            {%- if kind in ('root', 'child') %}
            IFF(IS_ARRAY({{ node }}:"$") OR IS_OBJECT({{ node }}:"$"), '', {{ node }}:"$"::STRING)
            {%- elif kind == 'root_outer' and not childElement %}
            {{ src }}::STRING
            {%- elif kind in ('root_outer', 'child_outer') %}
            {#- Snowflake re-serialises the element and writes ' in attribute values as &apos;; it
                always quotes attributes with ", so a plain ' is valid and matches the source -#}
            REPLACE(TO_VARCHAR({{ node }}), '&apos;', '''')
            {%- elif kind in ('root_attr', 'child_attr') %}
            GET({{ node }}, '@{{ c['attr'] | replace("'", "''") }}')::STRING
            {%- else -%}
            {{ exceptions.raise_compiler_error("XMLParse: unknown output column kind '" ~ kind ~ "'") }}
            {%- endif %} as {{ '"' ~ (c['name'] | replace('"', '""')) ~ '"' }}
            {%- endfor %}
        {%- if childElement %}
        from (
            {#- every element tagged childElement, keeping only the outermost when they nest. The
                not-exists filter is most of this mode's cost (~2.5x the recursive flatten alone). -#}
            with matches as (
                select s.*, f.seq as "__prophecy_seq", f.path as "__prophecy_path", f.value as {{ el }}
                from (
                    select *, {{ parsed }} as {{ doc }}
                    from {{ relation_list | join(', ') }}
                ) as s,
                lateral flatten(input => s.{{ doc }}, recursive => true) as f
                where get(f.value, '@')::string = '{{ childElement | replace("'", "''") }}'
            )
            select * from matches as m
            where not exists (
                select 1 from matches as o
                where o."__prophecy_seq" = m."__prophecy_seq"
                  and o."__prophecy_path" <> m."__prophecy_path"
                  and startswith(m."__prophecy_path", o."__prophecy_path")
            )
        )
        {%- else %}
        from (
            select *, {{ parsed }} as {{ doc }}
            from {{ relation_list | join(', ') }}
        )
        {%- endif %}
    {%- endif -%}

{% endmacro %}

{%- macro duckdb__XMLParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema,
    outputColumns=[],
    includeSourceColumn=true,
    ignoreErrors=true,
    childElement='') -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {# Simple XML parsing implementation for DuckDB - returns original data #}
    SELECT * FROM {{ relation_list | join(', ') }}
{%- endmacro -%}