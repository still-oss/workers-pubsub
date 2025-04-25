import { shake } from 'radashi'
import { getTokenFromGCPServiceAccount } from './workers-jwt'
import { createPubSubMessage } from './utils'

export type ServiceAccountCredentials = {
  type: string
  project_id: string
  private_key_id: string
  private_key: string
  client_email: string
  client_id: string
  auth_uri: string
  token_uri: string
  auth_provider_x509_cert_url: string
  client_x509_cert_url: string
}

export type PubSubConfig = {
  serviceAccountJSON: ServiceAccountCredentials
}

export type PubSubMessage = {
  data?: unknown
  attributes?: Record<string, string>
  ordering_key?: string
}

export type PublishResponse = {
  messageIds: string[]
}

export function definePublisher(config: PubSubConfig) {
  const fetch = createFetch(config)

  return {
    async publish(topic: string, messages: PubSubMessage[]) {
      return fetch<PublishResponse>(`topics/${topic}:publish`, {
        method: 'POST',
        body: JSON.stringify({
          messages: messages.map(encodePubSubMessage),
        }),
        responseType: 'json',
      })
    },
    async delete(topic: string) {
      await fetch(`topics/${topic}`, {
        method: 'DELETE',
      })
    },
  }
}

function encodePubSubMessage(message: PubSubMessage) {
  if (
    message.data === undefined &&
    (!message.attributes || !Object.keys(message.attributes).length)
  ) {
    throw new Error('Expected non-empty data or at least one attribute.')
  }
  return shake({
    ...message,
    data:
      message.data !== undefined
        ? btoa(JSON.stringify(message.data))
        : undefined,
  })
}

export function defineSubscriber(config: PubSubConfig) {
  const fetch = createFetch(config)

  return {}
}

export class PubSubHttpError extends Error {
  name = 'PubSubHttpError'
  constructor(
    public request: Request,
    public response: Response,
  ) {
    super(`Request to Google PubSub failed: ${request.method} ${request.url}`)
  }
}

type FetchOptions = Omit<RequestInit, 'headers'> & {
  headers?: Record<string, string>
  responseType?: 'json' | null
}

function createFetch(config: PubSubConfig) {
  let headersPromise: Promise<Record<string, string>> | undefined

  async function fetcher<T>(
    path: string,
    options: FetchOptions & { responseType: 'json' },
  ): Promise<T>

  async function fetcher(
    path: string,
    options?: FetchOptions & { responseType?: null },
  ): Promise<Response>

  async function fetcher(
    path: string,
    options: FetchOptions = {},
  ): Promise<any> {
    const headers = await (headersPromise ||= createHeaders(config))
    const request = new Request(
      `https://pubsub.googleapis.com/v1/projects/${config.serviceAccountJSON.project_id}/${path}`,
      {
        ...options,
        headers: {
          ...headers,
          ...options.headers,
        },
      },
    )
    const response = await fetch(request)
    if (!response.ok) {
      throw new PubSubHttpError(request, response)
    }
    if (options.responseType === 'json') {
      return response.json()
    }
    return response
  }

  return fetcher
}

async function createHeaders(config: PubSubConfig) {
  const aud = 'https://pubsub.googleapis.com/google.pubsub.v1.Publisher'

  const token = await getTokenFromGCPServiceAccount({
    serviceAccountJSON: config.serviceAccountJSON,
    aud,
  })

  return {
    Authorization: `Bearer ${token}`,
  }
}
