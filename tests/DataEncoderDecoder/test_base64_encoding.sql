-- Test Case: Hex encoding with inplace substitution
-- Functional scenario: Encoding sensitive data for storage
with actual as (
  {{ prophecy_basics.DataEncoderDecoder(
      relation_name=ref('simple_text'),
      column_names=['message'],
      remaining_columns='id, description',
      enc_dec_method='hex',
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
  select 1 as id, 'Simple greeting' as description, '48656c6c6f20576f726c64' as message union all
  select 2 as id, 'Basic test message' as description, '546573742044617461' as message union all
  select 3 as id, 'Testing encoding functions' as description, '456e636f64696e672054657374' as message
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
