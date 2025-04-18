

SELECT DISTINCT
    post.id as post_id,
    post.created_at,
    post_subject.subject_id,
    post_course.subject_id as course_id,
    course.institution_id,
    course.region_code,
    DATEADD(month, -6, post.created_at) as threshold_date
FROM
    "production"."production_studocu"."post" as post
LEFT JOIN
    "production"."production_studocu"."user_dismissed_post" as deleted_post
    ON post.id = deleted_post.post_id
LEFT JOIN 
    "production"."production_studocu"."post_subject" as post_subject
    ON post.id = post_subject.post_id
    AND post_subject.subject_type = 'Subject'
LEFT JOIN 
    "production"."production_studocu"."post_subject" as post_course
    ON post.id = post_course.post_id
    AND post_course.subject_type = 'Course'
LEFT JOIN
    "production"."production_studocu"."course" as course
    ON post_course.subject_id = course.id
LEFT JOIN
    "production"."production_studocu"."comment_subject" as comment_subject
    ON post.id = comment_subject.subject_id
LEFT JOIN 
    "production"."production_studocu"."comment" as comment
    ON comment_subject.comment_id = comment.id
LEFT JOIN
    "production"."production_studocu"."user" as expert
    ON comment.user_id = expert.id
WHERE
    post.type <> 'ORGANIC'
    AND post.deleted_at IS NULL
    AND deleted_post.post_id IS NULL   
    AND expert.role_id IN (19, 21) --experts only