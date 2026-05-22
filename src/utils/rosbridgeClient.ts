export interface RosbridgeSubscribeOptions {
    queueLength?: number;
    throttleRate?: number;
}

export interface RosbridgeClientOptions {
    host?: string;
    port?: number;
    url?: string;
}

export interface RosbridgeClientHandlers {
    onOpen?: () => void;
    onMessage?: (rawData: string) => void;
    onClose?: () => void;
    onError?: (error: Event) => void;
}

export interface RosbridgePublishMessage {
    op?: string;
    topic?: string;
    msg?: unknown;
}

export interface RosbridgeServiceResponseMessage {
    op?: string;
    id?: string;
    service?: string;
    values?: Record<string, unknown>;
    result?: boolean;
}

type RosbridgeMessage = Record<string, unknown>;

type RosbridgeEventMap = {
    open: () => void;
    message: (rawData: string) => void;
    close: () => void;
    error: (error: Event) => void;
};

export interface ServiceCallOptions {
    id?: string;
    timeoutMs?: number;
}

export class Message<TData = RosbridgeMessage> {
    constructor(public readonly data: TData) {}
}

export class ServiceRequest<TData = RosbridgeMessage> extends Message<TData> {
    constructor(data: TData = {} as TData) {
        super(data);
    }
}

function parseIncomingMessage(rawData: string): RosbridgePublishMessage | RosbridgeServiceResponseMessage | null {
    try {
        return JSON.parse(rawData) as RosbridgePublishMessage | RosbridgeServiceResponseMessage;
    } catch {
        return null;
    }
}

function isPublishMessage(
    payload: RosbridgePublishMessage | RosbridgeServiceResponseMessage,
): payload is RosbridgePublishMessage {
    return payload.op === 'publish';
}

function isServiceResponseMessage(
    payload: RosbridgePublishMessage | RosbridgeServiceResponseMessage,
): payload is RosbridgeServiceResponseMessage {
    return payload.op === 'service_response';
}

function createRequestId(prefix: string): string {
    return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
}

export class RosbridgeClient {
    private socket: WebSocket | null = null;
    private url: string;
    private readonly eventHandlers: { [K in keyof RosbridgeEventMap]: Set<RosbridgeEventMap[K]> } = {
        open: new Set(),
        message: new Set(),
        close: new Set(),
        error: new Set(),
    };

    constructor(options: RosbridgeClientOptions = {}) {
        const host = options.host ?? 'localhost';
        const port = options.port ?? 9090;
        this.url = options.url ?? `ws://${host}:${port}`;
    }

    get isConnected(): boolean {
        return this.isOpen();
    }

    run(): void {
        this.connect(this.url);
    }

    runForever(): void {
        this.run();
    }

    connect(url: string = this.url, handlers: RosbridgeClientHandlers = {}): void {
        this.url = url;
        this.disconnect();

        const socket = new WebSocket(url);
        this.socket = socket;

        socket.addEventListener('open', () => {
            if (this.socket !== socket) return;
            handlers.onOpen?.();
            this.emit('open');
        });

        socket.addEventListener('message', (event: MessageEvent) => {
            if (this.socket !== socket) return;
            if (typeof event.data !== 'string') return;
            handlers.onMessage?.(event.data);
            this.emit('message', event.data);
        });

        socket.addEventListener('close', () => {
            if (this.socket === socket) {
                this.socket = null;
            }
            handlers.onClose?.();
            this.emit('close');
        });

        socket.addEventListener('error', (error: Event) => {
            if (this.socket !== socket) return;
            handlers.onError?.(error);
            this.emit('error', error);
        });
    }

    onReady(handler: () => void): void {
        if (this.isOpen()) {
            handler();
            return;
        }

        const wrapped = () => {
            this.off('open', wrapped);
            handler();
        };
        this.on('open', wrapped);
    }

    on<K extends keyof RosbridgeEventMap>(event: K, handler: RosbridgeEventMap[K]): void {
        this.eventHandlers[event].add(handler);
    }

    off<K extends keyof RosbridgeEventMap>(event: K, handler: RosbridgeEventMap[K]): void {
        this.eventHandlers[event].delete(handler);
    }

    close(): void {
        this.disconnect();
    }

    terminate(): void {
        this.disconnect();
    }

    disconnect(): void {
        if (!this.socket) return;

        const socket = this.socket;
        this.socket = null;
        socket.close();
    }

    isOpen(): boolean {
        return !!this.socket && this.socket.readyState === WebSocket.OPEN;
    }

    isActive(): boolean {
        return !!this.socket && this.socket.readyState <= WebSocket.OPEN;
    }

    subscribe(topic: string, type: string, options: RosbridgeSubscribeOptions = {}): void {
        const { queueLength = 1, throttleRate = 0 } = options;
        this.send({
            op: 'subscribe',
            topic,
            type,
            queue_length: queueLength,
            throttle_rate: throttleRate,
        });
    }

    unsubscribe(topic: string): void {
        this.send({
            op: 'unsubscribe',
            topic,
        });
    }

    publish(topic: string, message: RosbridgeMessage): void {
        this.send({
            op: 'publish',
            topic,
            msg: message,
        });
    }

    callService(service: string, args: Record<string, unknown>, id: string, serviceType?: string): void {
        const request: RosbridgeMessage = {
            op: 'call_service',
            service,
            args,
            id,
        };
        if (serviceType) request.type = serviceType;
        this.send(request);
    }

    private send(message: RosbridgeMessage): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        this.socket.send(JSON.stringify(message));
    }

    private emit<K extends keyof RosbridgeEventMap>(event: K, ...args: Parameters<RosbridgeEventMap[K]>): void {
        for (const handler of Array.from(this.eventHandlers[event])) {
            (handler as (...params: Parameters<RosbridgeEventMap[K]>) => void)(...args);
        }
    }
}

export class Topic<TMessage = RosbridgeMessage> {
    private readonly subscribers = new Set<(message: TMessage) => void>();
    private readonly onClientMessage = (rawData: string) => {
        const payload = parseIncomingMessage(rawData);
        if (!payload || !isPublishMessage(payload)) return;
        if (payload.topic !== this.name) return;
        if (payload.msg === undefined) return;

        for (const subscriber of Array.from(this.subscribers)) {
            subscriber(payload.msg as TMessage);
        }
    };

    constructor(
        private readonly client: RosbridgeClient,
        private readonly name: string,
        private readonly messageType: string,
        private readonly options: RosbridgeSubscribeOptions = {},
    ) {}

    subscribe(callback: (message: TMessage) => void): void {
        this.subscribers.add(callback);
        if (this.subscribers.size > 1) return;

        this.client.on('message', this.onClientMessage);
        this.client.subscribe(this.name, this.messageType, this.options);
    }

    unsubscribe(callback?: (message: TMessage) => void): void {
        if (callback) {
            this.subscribers.delete(callback);
        } else {
            this.subscribers.clear();
        }

        if (this.subscribers.size > 0) return;
        this.client.off('message', this.onClientMessage);
        this.client.unsubscribe(this.name);
    }

    publish(message: Message<TMessage> | TMessage): void {
        const payload = message instanceof Message ? message.data : message;
        this.client.publish(this.name, payload as Record<string, unknown>);
    }
}

export class Service<
    TRequest = RosbridgeMessage,
    TResponse = RosbridgeMessage,
> {
    constructor(
        private readonly client: RosbridgeClient,
        private readonly name: string,
        private readonly serviceType?: string,
    ) {}

    call(
        request: ServiceRequest<TRequest> | TRequest,
        callback?: (response: TResponse) => void,
        options: ServiceCallOptions = {},
    ): Promise<TResponse> {
        if (!this.client.isOpen()) {
            return Promise.reject(new Error('ROS bridge is not connected.'));
        }

        const requestData = request instanceof ServiceRequest ? request.data : request;
        const id = options.id ?? createRequestId('service');

        const promise = new Promise<TResponse>((resolve, reject) => {
            let timeoutHandle: number | null = null;
            const onClientMessage = (rawData: string) => {
                const payload = parseIncomingMessage(rawData);
                if (!payload || !isServiceResponseMessage(payload)) return;
                if (payload.id !== id) return;

                this.client.off('message', onClientMessage);
                if (timeoutHandle !== null) {
                    window.clearTimeout(timeoutHandle);
                    timeoutHandle = null;
                }

                if (payload.result !== true) {
                    reject(new Error(`Service ${this.name} call failed.`));
                    return;
                }

                resolve((payload.values ?? {}) as TResponse);
            };

            this.client.on('message', onClientMessage);

            if (typeof options.timeoutMs === 'number' && options.timeoutMs > 0) {
                timeoutHandle = window.setTimeout(() => {
                    this.client.off('message', onClientMessage);
                    reject(new Error(`Service ${this.name} call timed out.`));
                }, options.timeoutMs);
            }

            this.client.callService(this.name, requestData as Record<string, unknown>, id, this.serviceType);
        });

        if (callback) {
            promise.then(callback).catch(() => {});
        }

        return promise;
    }
}