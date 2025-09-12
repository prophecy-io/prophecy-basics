-- Test FuzzyMatch with multiple match fields
-- This test verifies that the macro works with name and address matching
with actual as (
    {{ prophecy_basics.FuzzyMatch(
        relation='fuzzy_match_data',
        mode='PURGE',
        sourceIdCol='id',
        recordIdCol='id',
        matchFields={'name': ['name'], 'address': ['address']},
        matchThresholdPercentage=70,
        includeSimilarityScore=true
    ) }}
),
expected as (
    select '2' as record_id1, '1' as record_id2, 81.67 as similarity_score
    union all
    select '6' as record_id1, '5' as record_id2, 78.02 as similarity_score
    union all
    select '4' as record_id1, '3' as record_id2, 70.54 as similarity_score
)
select * from actual
except
select * from expected
