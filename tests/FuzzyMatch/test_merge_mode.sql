-- Test FuzzyMatch in MERGE mode
-- This test verifies that the macro finds similar records across different sources
with actual as (
    {{ prophecy_basics.FuzzyMatch(
        relation='fuzzy_merge_data',
        mode='MERGE',
        sourceIdCol='source_id',
        recordIdCol='record_id',
        matchFields={'name': ['name']},
        matchThresholdPercentage=80,
        includeSimilarityScore=true
    ) }}
),
expected as (
    select '1' as record_id1, '2' as record_id2, 90.0 as similarity_score
    union all
    select '3' as record_id1, '4' as record_id2, 85.0 as similarity_score
    union all
    select '5' as record_id1, '6' as record_id2, 88.0 as similarity_score
)
select * from actual
except
select * from expected
