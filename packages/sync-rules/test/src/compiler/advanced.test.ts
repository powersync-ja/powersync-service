import { describe, expect, test } from 'vitest';
import { serializeSyncPlan } from '../../../src/index.js';
import { compileSingleStreamAndSerialize, compileToSyncPlanWithoutErrors } from './utils.js';

describe('new sync stream features', () => {
  test('order-independent parameters', () => {
    // We should be able to merge buckets with the same parameters in a different order. Obviously, it's important that
    // we re-order the instantiation as well to keep it consistent.
    expect(
      compileSingleStreamAndSerialize(
        "SELECT * FROM stores WHERE region = subscription.parameter('region') AND org = auth.parameter('org')",
        "SELECT * FROM products WHERE org = auth.parameter('org') AND region = subscription.parameter('region')"
      )
    ).toMatchSnapshot();
  });

  describe('joins feedback', () => {
    // Anonymized queries that have been shared with us via the "support for multiple table joins in Sync Rules" Google
    // form. There were 13 responses.
    // responses 2, 7 don't have an exact query.
    // response 3 tries to select from multiple tables and would require ivm
    // response 6 uses aggregate functions (would require advanced ivm).
    // we should support response 12 in the future, but it's currently skipped

    test('response 1', () => {
      expect(
        compileSingleStreamAndSerialize(`
          SELECT u.*
            FROM users u
            JOIN group_memberships gm1 ON u.id = gm1.user_id
            JOIN group_memberships gm2 ON gm1.group_id = gm2.group_id
            WHERE gm2.user_id = auth.user_id();
        `)
      ).toMatchSnapshot();
    });

    test('response 4', () => {
      expect(
        compileSingleStreamAndSerialize(`
          SELECT m.* 
            FROM message m
            JOIN roles srm
              ON m.organization_id = srm.organization_id 
              WHERE srm.account_id=auth.user_id() 
              AND srm.role_id='ORGANIZATION_LEADER'
        `)
      ).toMatchSnapshot();
    });

    test('response 5', () => {
      const parameter = `SELECT users.family_id
            FROM users
            INNER JOIN user_auth_mappings ON users.id = user_auth_mappings.user_id
            WHERE user_auth_mappings.auth_id = auth.user_id()`;

      const queryA = `SELECT * FROM users WHERE family_id IN (${parameter})`;
      const queryB = `SELECT * FROM families WHERE id IN (${parameter})`;

      // Note that this is able to create a single bucket by de-duplicating a complex parameter instantiation.
      expect(compileSingleStreamAndSerialize(queryA, queryB)).toMatchSnapshot();
    });

    test('response 8', () => {
      // This would also be supported by old sync streams.
      expect(
        compileSingleStreamAndSerialize(
          `SELECT * FROM events WHERE id IN (SELECT event_id FROM event_users WHERE user_id = auth.user_id())`
        )
      ).toMatchSnapshot();
    });

    test('response 9', () => {
      const streamA = `
SELECT
    u.*
FROM
    public.users AS u
JOIN
    public.user_organization_map AS uom_colleagues ON u.id = uom_colleagues.user_id
WHERE
    uom_colleagues.organization_id IN (
        SELECT organization_id
        FROM public.user_organization_map
        WHERE user_id = auth.user_id()
    );
        `;
      const compiledA = compileSingleStreamAndSerialize(streamA);
      expect(compiledA).toMatchSnapshot();

      // The user provided an equivalent stream definition not using subqueries.
      const streamB = `
SELECT
    DISTINCT u.*
FROM
    public.user_organization_map AS uom1
JOIN
    public.user_organization_map AS uom2 ON uom1.organization_id = uom2.organization_id
JOIN
    public.users AS u ON uom2.user_id = u.id
WHERE
    uom1.user_id = auth.user_id()
        `;

      // We're able to recognize the two definitions are equivalent, they compile into the same sync plan.
      expect(compileSingleStreamAndSerialize(streamB)).toStrictEqual(compiledA);
    });

    test('response 10', () => {
      const stream = `
SELECT
    a.*
FROM
    public.users AS u
JOIN
    public.addresses AS a ON u.address_id = a.id
WHERE
    u.id IN (
        SELECT
            DISTINCT uom.user_id
        FROM
            public.user_organization_map AS uom
        WHERE
            uom.organization_id IN (
                SELECT
                    organization_id
                FROM
                    public.user_organization_map
                WHERE
                    user_id = auth.parameter('app_metadata.user_id')
            )
    );
        `;
      expect(compileSingleStreamAndSerialize(stream)).toMatchSnapshot();
    });

    test('response 11', () => {
      // This would already be supported with old sync streams by using a subquery
      const stream = `
SELECT t.*
FROM ticket t
    JOIN ticket_detail_item item ON item.ticket_id = t.id
    WHERE item.user_id = auth.user_id();
        `;
      expect(compileSingleStreamAndSerialize(stream)).toMatchSnapshot();
    });

    test('response 12', () => {
      const stream = `
SELECT p.* FROM profile p
WHERE p.id IN (
    SELECT ppl.profile_id 
        FROM profile_project_link ppl
        JOIN project pr ON ppl.project_id = pr.id
        WHERE auth.user_id() in pr.download_profiles
            AND ppl.membership IS NOT NULL
            AND ppl.active = true
)`;
      expect(compileSingleStreamAndSerialize(stream)).toMatchSnapshot();
    });

    test('response 13', () => {
      const stream = `
select
  c.*
from user_assignment_scope uas
join assignment a
  on a.id = uas.assignment_id
join assignment_checkpoint ac
  on ac.assignment_id = a.id
join checkpoint c
  on c.id = ac.checkpoint_id
where uas.user_id = auth.user_id()
  and a.active = true;`;

      expect(compileSingleStreamAndSerialize(stream)).toMatchSnapshot();
    });
  });

  test('in array', () => {
    expect(
      compileSingleStreamAndSerialize(`SELECT * FROM notes WHERE state IN ARRAY['public', 'archived']`)
    ).toMatchSnapshot();
  });

  test('not in array', () => {
    expect(
      compileSingleStreamAndSerialize(`SELECT * FROM notes WHERE state NOT IN ARRAY['public', 'archived']`)
    ).toMatchSnapshot();
  });

  test('in json array', () => {
    expect(
      compileSingleStreamAndSerialize(`SELECT * FROM notes WHERE state IN '["public", "archived"]'`)
    ).toMatchSnapshot();
  });

  test('not in json array', () => {
    expect(
      compileSingleStreamAndSerialize(`SELECT * FROM notes WHERE state NOT IN '["public", "archived"]'`)
    ).toMatchSnapshot();
  });

  test('in array and additional filter in subquery', () => {
    expect(
      compileSingleStreamAndSerialize(`
SELECT * FROM notes
  WHERE owner_id IN (
    SELECT users.id
    FROM users
    WHERE users.state IN '["public", "archived"]'
      AND users.org = auth.parameter('org')
  )
`)
    ).toMatchSnapshot();
  });

  describe('table-valued functions', () => {
    test('static filter', () => {
      expect(
        compileSingleStreamAndSerialize(`SELECT * FROM posts WHERE 'important' IN posts.descriptions`)
      ).toMatchSnapshot();
    });

    test('partition on data', () => {
      expect(
        compileSingleStreamAndSerialize(
          `SELECT * FROM posts WHERE subscription.parameter('tag') IN (SELECT value FROM json_each(posts.tags))`
        )
      ).toMatchSnapshot();
    });

    test('partition on parameter lookup', () => {
      expect(
        compileSingleStreamAndSerialize(`SELECT 
            users.* FROM users, orgs, json_each(orgs.members) as members
          WHERE users.id = members.value AND orgs.id = auth.parameter('org')`)
      ).toMatchSnapshot();
    });
  });

  test('IN operator with static left clause', () => {
    expect(
      compileSingleStreamAndSerialize("SELECT * FROM issues WHERE 'static' IN (SELECT id FROM users WHERE is_admin)")
    ).toMatchSnapshot();
  });

  test('IN operator with two static clauses', () => {
    expect(
      compileSingleStreamAndSerialize("SELECT * FROM issues WHERE 'issues' IN auth.parameter('synced_tables')")
    ).toMatchSnapshot();
  });

  test('json-each of cte', () => {
    // Regression test for https://discord.com/channels/1138230179878154300/1479042473316847746/1479049290830839928.
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  manually_converted:
    auto_subscribe: true
    with:
      available_projects: |
          SELECT project
          FROM "ProjectInvitation"
          WHERE "appliedTo" != ''
          AND (auth.parameters() ->> 'haystack_id') IN "appliedTo"
          AND "status" = 'CLAIMED'
          AND connection.parameter('use_streams') != 1
          AND connection.parameter('use_streams') != '1'
          AND 'Scene' IN connection.parameter('synced_objects')
    queries:
      - |
        SELECT "Scene"._id as id,
        unixepoch("Scene"."createdOn") as "createdOnSortable",
        IFNULL("Scene".archived, 0) as "archived",
        "Scene".*
        FROM "Scene", available_projects
        WHERE "Scene".project IN available_projects.project
`);
    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });

  test('checking for existence in two CTEs', () => {
    // Regression test for https://discord.com/channels/1138230179878154300/1480494423417688105/1480494813684961405,
    // slightly simplified.
    const plan = compileToSyncPlanWithoutErrors(`
config:
  edition: 3

streams:
  rbac_stream:
    accept_potentially_dangerous_queries: true
    with:
      user_project_permissions: SELECT perm FROM project_permissions WHERE "user" = auth.user_id()
      user_global_permissions: SELECT perm FROM global_permissions WHERE "user" = auth.user_id()

    query: |
      SELECT "Scene".*
      FROM "Scene"
      WHERE 
        "Scene".project = subscription.parameter('project')
        AND (
            'a' IN user_global_permissions
            OR 'b' IN user_project_permissions
        )
`);

    // There should only be a single bucket, but two ways to get there (thanks to the OR).
    expect(plan.buckets).toHaveLength(1);
    expect(plan.streams).toHaveLength(1);
    expect(plan.streams[0].queriers).toHaveLength(2);

    expect(serializeSyncPlan(plan)).toMatchSnapshot();
  });
});
