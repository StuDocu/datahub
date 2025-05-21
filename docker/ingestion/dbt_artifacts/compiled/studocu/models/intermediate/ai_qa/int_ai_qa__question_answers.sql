WITH first_question_answers AS (
    SELECT
        comment_subject.subject_id as post_id,
        comment.id,
        comment.comment, --can we remove html tags here?
        comment.created_at,
        ROW_NUMBER() OVER (PARTITION BY comment_subject.subject_id ORDER BY comment.created_at) as rank
    FROM 
        "production"."production_studocu"."comment_subject" as comment_subject
    INNER JOIN
        "production"."production_studocu"."comment" as comment
        ON comment.id = comment_subject.comment_id
    INNER JOIN 
        "production"."production_studocu"."post" as post
        ON comment_subject.subject_id = post.id
    WHERE
        post.type <> 'ORGANIC'
        AND comment_subject.subject_type = 'Post'
),
answer_types AS (
    SELECT 
        comment_subject.subject_id as post_id,
        COUNT(DISTINCT comment.id) as count_comments,
        COUNT(DISTINCT CASE WHEN comment.user_id = post.user_id THEN comment.id END) as count_comments_user,
        COUNT(DISTINCT CASE WHEN comment.user_id = assigned_questions.expert_id THEN comment.id END) as count_comments_expert
    FROM 
        "production"."production_studocu"."comment_subject" as comment_subject
    INNER JOIN
        "production"."production_studocu"."comment" as comment
        ON comment.id = comment_subject.comment_id
    INNER JOIN
        "production"."production_studocu"."post" as post
        ON comment_subject.subject_id = post.id
    LEFT JOIN
        "production"."intermediate"."assigned_questions" as assigned_questions
        ON comment_subject.subject_id = assigned_questions.post_id
    WHERE
        post.type <> 'ORGANIC'
        AND comment_subject.subject_type = 'Post'
    GROUP BY    
        1
)
SELECT
    first_question_answers.post_id,
    first_question_answers.id as first_answer_id,
    first_question_answers.created_at as first_answer_created_at,
    first_question_answers.comment as first_answer_content,
    'https://www.studocu.com/en-gb/messages/question/' || first_question_answers.post_id as question_url,
    answer_types.count_comments,
    answer_types.count_comments_user,
    answer_types.count_comments_expert
FROM
    first_question_answers
INNER JOIN
    answer_types
    ON first_question_answers.post_id = answer_types.post_id
WHERE   
    first_question_answers.rank = 1