-- Test FuzzyMatch in PURGE mode
-- This test verifies that the macro finds similar records using name matching
with actual as (
    {{ prophecy_basics.FuzzyMatch(
        relation='fuzzy_match_data',
        mode='PURGE',
        sourceIdCol='id',
        recordIdCol='id',
        matchFields={'name': ['name']},
        matchThresholdPercentage=80,
        includeSimilarityScore=true
    ) }}
),
expected as (
    select '2' as record_id1, '1' as record_id2, 90.0 as similarity_score
)
select * from actual
except
select * from expected
