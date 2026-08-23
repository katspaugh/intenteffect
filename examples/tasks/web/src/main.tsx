import { createRoot } from 'react-dom/client'
import { createClient } from '@intenteffect/client'
import { IntentEffectProvider } from '@intenteffect/react'
import { App } from './App.js'

const client = createClient()

createRoot(document.getElementById('root')!).render(
  <IntentEffectProvider client={client}>
    <App />
  </IntentEffectProvider>,
)
