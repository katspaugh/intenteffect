import { z } from 'zod'
import { event, intent, projection } from '@intenteffect/core'

/* ---------------- events: "X happened" ---------------- */

export const taskCreated = event(
  'task.created',
  z.object({ id: z.string(), title: z.string() }),
)

export const taskRenamed = event(
  'task.renamed',
  z.object({ id: z.string(), title: z.string() }),
)

export const taskDeleted = event('task.deleted', z.object({ id: z.string() }))

/* ---------------- intents: "please make X happen" ---------------- */

export const createTask = intent(
  'task.create',
  z.object({ id: z.string(), title: z.string().min(1) }),
  { emits: [taskCreated] },
)

export const renameTask = intent(
  'task.rename',
  z.object({ id: z.string(), title: z.string().min(1) }),
  { emits: [taskRenamed] },
)

export const deleteTask = intent('task.delete', z.object({ id: z.string() }), {
  emits: [taskDeleted],
})

/* ---------------- projections: synchronized views ---------------- */

const task = z.object({ id: z.string(), title: z.string() })
export type Task = z.output<typeof task>

export const taskList = projection({
  name: 'tasks.list',
  result: z.array(task),
})
  .on(taskCreated, (tasks, data) =>
    tasks.some((t) => t.id === data.id) ? tasks : [...tasks, data],
  )
  .on(taskRenamed, (tasks, data) =>
    tasks.map((t) => (t.id === data.id ? { ...t, title: data.title } : t)),
  )
  .on(taskDeleted, (tasks, data) => tasks.filter((t) => t.id !== data.id))
