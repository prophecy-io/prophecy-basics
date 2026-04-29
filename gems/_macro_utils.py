# Shared relation-name resolution for MacroSpec gems.
# Keep in sync with: prophecy/modules/common/core/src/main/resources/pyspark_sql_gems/_macro_utils.py

import re


def get_relation_names(component, context):
    """
    Compute relation names for a macro gem's input ports.

    If a port slug is not a default 'inN' pattern (e.g. 'in0', 'in1'), it was
    pre-set by the platform to the suffixed upstream label (e.g. 'script_gem_out0')
    and should be used directly as the relation name.

    Otherwise, falls back to graph traversal to find the upstream node's label.
    """
    relation_names = []
    for input_port in component.ports.inputs:
        if input_port.slug and not re.match(r'^in\d+$', input_port.slug):
            relation_names.append(input_port.slug)
        else:
            upstream_label = _get_upstream_label(input_port, context)
            relation_names.append(upstream_label)
    return relation_names


def _get_upstream_label(input_port, context):
    """Traverse graph connections to find the upstream node's label for a given input port."""
    for connection in context.graph.connections:
        if connection.targetPort == input_port.id:
            upstream_node = context.graph.nodes.get(connection.source)
            if upstream_node is not None and upstream_node.label is not None:
                return upstream_node.label
    return ""
