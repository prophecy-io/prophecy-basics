-- Test Case: Base64 decoding with inplace substitution
-- Functional scenario: Decoding stored data back to original format
with actual as (
  {{ prophecy_basics.DataEncoderDecoder(
      relation_name=ref('encoded_data'),
      column_names=['encoded_name'],
      remaining_columns='id, encoded_email',
      enc_dec_method='unbase64',
      enc_dec_charSet='',
      aes_enc_dec_secretScope_key='',
      aes_enc_dec_secretKey_key='',
      aes_enc_dec_mode='',
      aes_enc_dec_secretScope_aad='',
      aes_enc_dec_secretKey_aad='',
      aes_enc_dec_secretScope_iv='',
      aes_enc_dec_secretKey_iv='',
      prefix_suffix_opt='',
      change_col_name='inplace_substitute',
      prefix_suffix_val=''
  ) }}
),
expected as (
  select 1 as id, 'dGVzdEBleGFtcGxlLmNvbQ==' as encoded_email, 'Hello World' as encoded_name union all
  select 2 as id, 'ZXhhbXBsZUB0ZXN0LmNvbQ==' as encoded_email, 'Test Data' as encoded_name union all
  select 3 as id, 'ZGVtb0B0ZXN0LmNvbQ==' as encoded_email, 'Encoding Test' as encoded_name
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
