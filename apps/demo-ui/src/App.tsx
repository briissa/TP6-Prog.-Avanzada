import { FormEvent, useEffect, useState } from 'react';
import { io } from 'socket.io-client';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

type Activation = {
  id: string;
  customerId: string;
  planId: string;
  status: string;
  history: Array<{ eventId: string; eventType: string; at: string }>;
};

export default function App() {
  const [customerId, setCustomerId] = useState('');
  const [planId, setPlanId] = useState('');
  const [simulateFailure, setSimulateFailure] = useState('none');
  const [activation, setActivation] = useState<Activation | null>(null);
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    const socket = io(API_URL);
    socket.on('activation.updated', (updated: Activation) => {
      setActivation((current) => (current?.id === updated.id ? updated : current));
    });
    return () => {
      socket.disconnect();
    };
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError('');
    setSubmitting(true);
    try {
      const response = await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });
      if (!response.ok) throw new Error(`No se pudo crear la activación (${response.status})`);
      const created = (await response.json()) as { activationId: string };
      const current = await fetch(`${API_URL}/activations/${created.activationId}`);
      if (!current.ok) throw new Error('No se pudo consultar el estado de la activación');
      setActivation((await current.json()) as Activation);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main>
      <section className="panel">
        <p className="eyebrow">Sistemas distribuidos · TP6</p>
        <h1>Activación de servicios</h1>
        <p className="intro">Contratá un plan y seguí cómo los servicios reaccionan a los eventos.</p>
        <form onSubmit={submit}>
          <label>
            Cliente
            <input
              value={customerId}
              onChange={(event) => setCustomerId(event.target.value)}
              placeholder="cliente-001"
              required
            />
          </label>
          <label>
            Plan
            <input
              value={planId}
              onChange={(event) => setPlanId(event.target.value)}
              placeholder="plan-premium"
              required
            />
          </label>
          <label>
            Simular fallo
            <select value={simulateFailure} onChange={(event) => setSimulateFailure(event.target.value)}>
              <option value="none">Ninguno</option>
              <option value="billing">Facturación</option>
              <option value="provisioning">Aprovisionamiento</option>
            </select>
          </label>
          <button disabled={submitting}>{submitting ? 'Enviando…' : 'Contratar plan'}</button>
        </form>
        {error && <p className="error">{error}</p>}
      </section>

      {activation && (
        <section className="panel timeline">
          <div className="timeline-heading">
            <div>
              <p className="eyebrow">Seguimiento en vivo</p>
              <h2>{activation.id}</h2>
            </div>
            <span className={`status status-${activation.status.toLowerCase()}`}>{activation.status}</span>
          </div>
          <ol>
            {activation.history.map((item) => (
              <li key={item.eventId}>
                <span className="event-name">{item.eventType}</span>
                <time>{new Date(item.at).toLocaleTimeString()}</time>
              </li>
            ))}
          </ol>
        </section>
      )}
    </main>
  );
}
