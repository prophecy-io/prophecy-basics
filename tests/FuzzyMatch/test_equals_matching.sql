-- Test FuzzyMatch with EQUALS matching for phone numbers
-- This test verifies that exact matches work correctly
with actual as (
    {{ prophecy_basics.FuzzyMatch(
        relation='fuzzy_match_data',
        mode='PURGE',
        sourceIdCol='id',
        recordIdCol='id',
        matchFields={'phone': ['phone']},
        matchThresholdPercentage=100,
        includeSimilarityScore=true
    ) }}
),
expected as (
    select '2' as record_id1, '1' as record_id2, 100.0 as similarity_score
    union all
    select '4' as record_id1, '3' as record_id2, 100.0 as similarity_score
    union all
    select '6' as record_id1, '5' as record_id2, 100.0 as similarity_score
)
select * from actual
except
select * from expected
