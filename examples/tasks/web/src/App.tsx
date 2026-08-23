import { useState, type FormEvent } from 'react'
import { useConnectionStatus, useProjection, useSend } from '@intenteffect/react'
import { createTask, deleteTask, renameTask, taskList } from '../../shared/contracts.js'

export function App() {
  const tasks = useProjection(taskList)
  const send = useSend()
  const status = useConnectionStatus()
  const [title, setTitle] = useState('')
  const [lastError, setLastError] = useState<string | null>(null)

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    if (!title.trim()) return
    const result = await send(createTask, {
      id: crypto.randomUUID(),
      title: title.trim(),
    })
    if (result.ok) {
      setTitle('')
      setLastError(null)
    } else {
      setLastError(result.error.message)
    }
  }

  async function onRename(id: string, current: string) {
    const next = prompt('Rename task', current)
    if (!next || next === current) return
    const result = await send(renameTask, { id, title: next })
    setLastError(result.ok ? null : result.error.message)
  }

  async function onDelete(id: string) {
    const result = await send(deleteTask, { id })
    setLastError(result.ok ? null : result.error.message)
  }

  return (
    <main>
      <h1>IntentEffect · Tasks</h1>
      <p className={`status ${status}`}>{status}</p>

      <form onSubmit={onCreate}>
        <input
          type="text"
          placeholder="What needs doing?"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
        />
        <button type="submit">Add</button>
      </form>

      {lastError && <p role="alert">⚠ {lastError}</p>}

      {tasks.status === 'loading' && <p>Loading…</p>}
      {tasks.status === 'error' && <p role="alert">⚠ {tasks.error.message}</p>}
      {tasks.status === 'ready' && (
        <ul>
          {tasks.data.map((task) => (
            <li key={task.id}>
              <span>{task.title}</span>
              <button onClick={() => onRename(task.id, task.title)}>Rename</button>
              <button onClick={() => onDelete(task.id)}>Delete</button>
            </li>
          ))}
          {tasks.data.length === 0 && <li>No tasks yet — open a second tab and add some.</li>}
        </ul>
      )}
    </main>
  )
}
