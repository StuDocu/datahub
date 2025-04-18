with payout_extra_uni as 
(       
    select 
        p.institution_id as institution_id,
        sum(case when p.created_at >= c.created_at then amount_eur else 0 end) as docs_payout_extra_eur
    from "production"."production_studocu"."payout" p

    inner join "production"."production_studocu"."warplan_institution_comment" c on c.institution_id = p.institution_id
    inner join "production"."production_studocu"."institution" i on i.id = p.institution_id
    where p.status in ('SUCCESS')
	    and i.active = 1
	    and i.deleted = 0
        and i.merged_into_id is NULL
        and c.comment_subtype = 'EB'
        and c.deleted_at is NULL

    group by 1
), 
payout_extra_merged_uni as 
(       
    select 

        i.merged_into_id as merged_into_id,
        sum(case when p.created_at >= c.created_at then amount_eur else 0 end) as docs_payout_extra_eur
    from "production"."production_studocu"."payout" p
    inner join "production"."production_studocu"."warplan_institution_comment" c on c.institution_id = p.institution_id
    inner join "production"."production_studocu"."institution" i on i.id = p.institution_id
    where p.status in ('SUCCESS')
        and i.merged_into_id is not NULL
        and c.comment_subtype = 'EB'
        and c.deleted_at is NULL
        
    group by 1
)

select 
    uni.institution_id,
    max(uni.docs_payout_extra_eur + coalesce(merged_uni.docs_payout_extra_eur,0)) as total_docs_payout_extra_eur
from 
    payout_extra_uni as uni
left join 
    payout_extra_merged_uni as merged_uni on merged_uni.merged_into_id = uni.institution_id
group by 1