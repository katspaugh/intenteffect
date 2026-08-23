import { createTasksApp } from './app.js'

const connectionString =
  process.env.DATABASE_URL ?? 'postgres://dev:dev@localhost:5432/intenteffect'
const port = Number(process.env.PORT ?? 3001)

const { app } = await createTasksApp(connectionString)
await app.listen(port)
console.log(`IntentEffect tasks server listening on :${port}`)
