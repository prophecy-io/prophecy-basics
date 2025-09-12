-- Test Case: Multiple column hex encoding with suffix
-- Functional scenario: Encoding multiple sensitive columns
with actual as (
  {{ prophecy_basics.DataEncoderDecoder(
      relation_name=ref('encoder_test_data'),
      column_names=['name', 'email'],
      remaining_columns='id, phone, ssn',
      enc_dec_method='hex',
      enc_dec_charSet='',
      aes_enc_dec_secretScope_key='',
      aes_enc_dec_secretKey_key='',
      aes_enc_dec_mode='',
      aes_enc_dec_secretScope_aad='',
      aes_enc_dec_secretKey_aad='',
      aes_enc_dec_secretScope_iv='',
      aes_enc_dec_secretKey_iv='',
      prefix_suffix_opt='Suffix',
      change_col_name='prefix_suffix_substitute',
      prefix_suffix_val='_encoded'
  ) }}
),
expected as (
  select 1 as id, '555-1234' as phone, '123-45-6789' as ssn, '4a6f686e20446f65' as name_encoded, '6a6f686e2e646f6540656d61696c2e636f6d' as email_encoded union all
  select 2 as id, '555-5678' as phone, '987-65-4321' as ssn, '4a616e6520536d697468' as name_encoded, '6a616e652e736d69746840656d61696c2e636f6d' as email_encoded union all
  select 3 as id, '555-9012' as phone, '456-78-9012' as ssn, '426f62204a6f686e736f6e' as name_encoded, '626f622e6a6f686e736f6e40656d61696c2e636f6d' as email_encoded union all
  select 4 as id, '555-3456' as phone, '789-01-2345' as ssn, '416c6963652042726f776e' as name_encoded, '616c6963652e62726f776e40656d61696c2e636f6d' as email_encoded union all
  select 5 as id, '555-7890' as phone, '234-56-7890' as ssn, '436861726c69652057696c736f6e' as name_encoded, '636861726c69652e77696c736f6e40656d61696c2e636f6d' as email_encoded
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
