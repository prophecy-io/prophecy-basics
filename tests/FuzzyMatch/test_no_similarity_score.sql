-- Test FuzzyMatch without similarity score
-- This test verifies that the macro works when includeSimilarityScore is false
with actual as (
    {{ prophecy_basics.FuzzyMatch(
        relation='fuzzy_match_data',
        mode='PURGE',
        sourceIdCol='id',
        recordIdCol='id',
        matchFields={'name': ['name']},
        matchThresholdPercentage=80,
        includeSimilarityScore=false
    ) }}
),
expected as (
    select '2' as record_id1, '1' as record_id2
)
select * from actual
except
select * from expected
