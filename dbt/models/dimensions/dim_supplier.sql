{{ config(unique_key='supplier_sk', pre_hook='{{ reconcile_snapshot() }}') }}

select
    {{ star_key('supplier', unknown_natural_key()) }} as supplier_sk,
    {{ unknown_natural_key() }} as supplier_natural_key,
    null::text as supplier_name,
    'UNKNOWN'::text as member_status
