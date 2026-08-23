import type pg from 'pg'
import { err, intentEffectError } from '@intenteffect/core'
import { createIntentEffect, type IntentEffectServer } from '@intenteffect/server'
import { createPostgresStore, type PostgresStore } from '@intenteffect/postgres'
import {
  createTask,
  deleteTask,
  renameTask,
  taskCreated,
  taskDeleted,
  taskList,
  taskRenamed,
} from '../shared/contracts.js'

export interface TasksApp {
  app: IntentEffectServer<Record<string, never>, pg.PoolClient>
  store: PostgresStore
}

export async function createTasksApp(connectionString: string): Promise<TasksApp> {
  const store = createPostgresStore({ connectionString })

  const migrated = await store.migrate()
  if (!migrated.ok) throw new Error(`migration failed: ${migrated.error.message}`)

  // The app keeps its ordinary source-of-truth table — no event sourcing.
  await store.pool.query(`
    create table if not exists tasks (
      id text primary key,
      title text not null,
      created_at timestamptz not null default now()
    )
  `)

  const app = createIntentEffect<Record<string, never>, pg.PoolClient>({
    store,
    tx: (raw) => raw as pg.PoolClient,
    // Event visibility hook — broadcast everything in this single-tenant demo:
    // authorizeEvent: (evt, ctx) => evt.tenantId === ctx.tenantId,
  })

  app.handle(createTask, async ({ input, tx, emit }) => {
    await tx.query(
      `insert into tasks (id, title) values ($1, $2) on conflict (id) do nothing`,
      [input.id, input.title],
    )
    emit(taskCreated, input)
  })

  app.handle(renameTask, async ({ input, tx, emit }) => {
    const updated = await tx.query(`update tasks set title = $2 where id = $1`, [
      input.id,
      input.title,
    ])
    if (updated.rowCount === 0) {
      return err(intentEffectError('not_found', `task ${input.id} does not exist`))
    }
    emit(taskRenamed, input)
  })

  app.handle(deleteTask, async ({ input, tx, emit }) => {
    await tx.query(`delete from tasks where id = $1`, [input.id])
    emit(taskDeleted, { id: input.id })
  })

  app.project(taskList, {
    query: async ({ tx }) => {
      const result = await tx.query(
        `select id, title from tasks order by created_at asc, id asc`,
      )
      return result.rows
    },
  })

  return { app, store }
}
