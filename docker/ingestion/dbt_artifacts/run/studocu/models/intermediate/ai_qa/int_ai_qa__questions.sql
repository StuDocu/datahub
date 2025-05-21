

  create view "production"."intermediate"."questions__dbt_tmp" as (
    WITH expert_question_views AS (
    SELECT
        *
    FROM    
        "production"."intermediate"."question_vieww"
),
dismissed_questions AS (
    SELECT
        *
    FROM
        "production"."intermediate"."dismissed_questions"
),
assigned_questions AS (
    SELECT
        *
    FROM
        "production"."intermediate"."assigned_questions"
),
question_answers AS (
    SELECT
        *
    FROM
        "production"."intermediate"."question_answers"
),
comment_last_rating AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_ai_qa__comment_last_rating"
),
rag_doc_answer_link AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_ai_qa__rag_doc_answer_link"
),
questions AS (
    SELECT
        post.id as post_id,
        post.type  as post_type,
        CASE
            WHEN post.deleted_at IS NOT NULL THEN 'deleted'
            WHEN dismissed_questions.dismissed_at IS NOT NULL THEN 'dismissed'
            WHEN question_answers.first_answer_created_at IS NOT NULL THEN 'answered'
            WHEN (assigned_questions.last_assigned_at >= assigned_questions.last_deleted_at) 
                 OR (assigned_questions.last_assigned_at IS NOT NULL AND assigned_questions.last_deleted_at IS NULL) THEN 'work in progress'
            WHEN assigned_questions.last_assigned_at IS NOT NULL THEN 'assigned but dropped'
            ELSE 'pending'
        END as "status",
        post.created_at,
        post.deleted_at,
        dismissed_questions.dismissed_at,
        assigned_questions.first_assigned_at,
        question_answers.first_answer_created_at as answered_at,
        post.is_anonymous,
        post.user_id,
        assigned_questions.expert_id,
        post.content as question_content, --can we remove html tags from content?
        question_answers.first_answer_content,
        question_answers.first_answer_id,
        rag.total_rag_references,
        rag.unique_rag_docs_referenced,
        rag.list_rag_doc_ids,
        p2p_answers_reviews.invalid_question_do_not_pay,
        p2p_answers_reviews.feedback,
        question_answers.question_url,
        comment_rating.created_at as rated_at, --double check if this is unique on comment_id
        comment_rating.rating,
        comment_rating.additional_information as rating_additional_info,
        dismissed_questions.dismissed_reason,
        dismissed_questions.dismissed_reason_additional_info,
        post_replied_to.test_rag,
        question_answers.count_comments,
        question_answers.count_comments_user,
        question_answers.count_comments_expert,
        subject.id as subject_id, --double check if post_subject is unique on post_id (for type 'subject')
        subject.name as subject_name,
        dim_group_subjects.subject_group_name,
        post_course.subject_id as course_id,
        course.name as course_name,
        institution.id as institution_id,
        institution.name as institution_name,
        institution.country_id as institution_country_id,
        institution.region_code as institution_region_code,
        institution.level as institution_level,
        institution.phase as institution_phase,
        grade.id as grade_id,
        grade.name as grade_name,
        degree.id as degree_id,
        degree.name as degree_name,
        degree.phase as degree_phase,
        country.name as institution_country_name,
        country_tier.tier,
        language.language_geocode as country_language,
        COALESCE(expert_question_views.lifetime_unique_page_views, 0) as lifetime_unique_page_views,
        COALESCE(expert_question_views.lifetime_unique_page_views_seo, 0) as lifetime_unique_page_views_seo,
        COALESCE(expert_question_views.lifetime_total_page_views, 0) as lifetime_total_page_views,
        COALESCE(expert_question_views.lifetime_total_page_views_seo, 0) as lifetime_total_page_views_seo,
        DATEDIFF(minute, post.created_at, assigned_questions.first_assigned_at) as minutes_created_to_assign,
        DATEDIFF(minute, post.created_at, question_answers.first_answer_created_at) as minutes_created_to_answer,
        DATEDIFF(minute, assigned_questions.first_assigned_at, question_answers.first_answer_created_at) as minutes_assign_to_answer,
        DATEDIFF(minute, post.created_at, LEAST(question_answers.first_answer_created_at, dismissed_questions.dismissed_at, post.deleted_at)) as minutes_created_to_resolved
    FROM
        "production"."production_studocu"."post" as post
    LEFT JOIN  
        dismissed_questions 
        ON post.id = dismissed_questions.post_id
    LEFT JOIN   
        assigned_questions
        ON post.id = assigned_questions.post_id
    LEFT JOIN
        question_answers
        ON post.id = question_answers.post_id
    LEFT JOIN
        comment_last_rating as comment_rating
        ON question_answers.first_answer_id = comment_rating.comment_id
    LEFT JOIN 
        "production"."production_studocu"."post_subject" as post_subject
        ON post.id = post_subject.post_id
        AND post_subject.subject_type = 'Subject'
    LEFT JOIN   
        "production"."production_studocu"."subject" as subject
        ON post_subject.subject_id = subject.id
    LEFT JOIN
        "production"."ai_qa"."fact_group_subjects" as fact_group_subjects
        ON post_subject.subject_id = fact_group_subjects.subject_id
    LEFT JOIN
        "production"."ai_qa"."dim_group_subjects" as dim_group_subjects
        ON fact_group_subjects.subject_group_id = dim_group_subjects.subject_group_id
    LEFT JOIN 
        "production"."production_studocu"."post_subject" as post_course
        ON post.id = post_course.post_id
        AND post_course.subject_type = 'Course'
    LEFT JOIN
        "production"."production_studocu"."course" as course
        ON post_course.subject_id = course.id
    LEFT JOIN 
        "production"."production_studocu"."post_subject" as post_institution
        ON post.id = post_institution.post_id
        AND post_institution.subject_type = 'Institution'
    LEFT JOIN
        "production"."production_studocu"."institution" as institution
        ON post_institution.subject_id = institution.id
    LEFT JOIN
        "production"."production_studocu"."country" as country
        ON institution.country_id = country.id
    LEFT JOIN  
        "production"."production_studocu"."language" as language
        ON institution.language_id = language.id
    LEFT JOIN 
        "raw_data"."general"."country_tier" as country_tier
        ON country.id = country_tier.country_id
    LEFT JOIN
        expert_question_views
        ON post.id = expert_question_views.post_id
    LEFT JOIN
        "production"."ai_qa"."p2p_answers_reviews" as p2p_answers_reviews
        ON post.id = p2p_answers_reviews.question_id
    LEFT JOIN 
        "production"."production_studocu"."grade" as grade 
        ON course.grade_id = grade.id
    LEFT JOIN 
        "production"."production_studocu"."degree" as degree 
        ON coalesce(course.degree_id,grade.degree_id) = degree.id
    LEFT JOIN 
        "production"."stg_mixpanel"."post_replied_to" as post_replied_to 
        ON post.id = post_replied_to.post_id
    LEFT JOIN 
        rag_doc_answer_link as rag
        ON question_answers.first_answer_id = rag.answer_id

    WHERE
        post.type <> 'ORGANIC'
)   
SELECT
    *
FROM 
    questions
  ) with no schema binding;
