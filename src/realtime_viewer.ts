import * as THREE from 'three';
import { Viewer } from './viewer';
import { CloudItem } from './items/CloudItem';
import { AxisItem } from './items/AxisItem';
import { NativeCloudItem } from './items/NativeCloudItem';
import { decodePointCloud2, inferColorModeFromFields } from './utils/pointCloud2Decode';
import { RosbridgeClient, Service, ServiceRequest, Topic } from './utils/rosbridgeClient';
import { makeLabel, makeTextInput, makeNumberInput, makeButton, buildNativeCloudItemSettings } from './viewer/settingsUI';
import type { RealtimeUrlOptions } from './realtimeUrlOptions';
import type {
    ColorMode,
    DecodedCloudChunk,
    OdomJson,
    PointCloud2Json,
    RealtimeTopicOptions,
} from './utils/realtimeTypes';

interface SlamStatusValues {
    slam?: boolean;
    livox?: boolean;
    record?: boolean;
    camera?: boolean;
}

interface SlamSwitchValues {
    success?: boolean;
}

/**
 * RealtimeViewer extends Viewer with ROS PointCloud2 realtime ingestion.
 * It is optimized for streaming append workloads instead of one-shot file loads.
 */
export class RealtimeViewer extends Viewer {
    private rosClient: RosbridgeClient | null = null;
    private cloudTopic: Topic<PointCloud2Json> | null = null;
    private odomTopic: Topic<OdomJson> | null = null;
    private rosbridgeUrl: string = `ws://${window.location.hostname}:9090`;
    private cloudTopicName: string = '/cloud_registered';
    private odomTopicName: string = '/odometry';
    private controlServiceName: string = '/web_mapping_manager/switch';
    private statusServiceName: string = '/web_mapping_manager/status';
    private autoRecord: boolean = false;
    private maxPointsPerScan: number = 1500;
    private rosUrlInput: HTMLInputElement | null = null;
    private autoRecordInput: HTMLInputElement | null = null;
    private maxScanInput: HTMLInputElement | null = null;
    private maxCloudInput: HTMLInputElement | null = null;
    private statusQueryInFlight = false;
    private readonly statusLedElements: Partial<Record<'slam' | 'livox' | 'record' | 'camera', HTMLSpanElement>> = {};
    private readonly statusTextElements: Partial<Record<'slam' | 'livox' | 'record' | 'camera', HTMLSpanElement>> = {};
    private statusPollTimer: number | null = null;
    private readonly statusPollIntervalMs = 1000;
    private mapColorMode: ColorMode | null = null;
    private readonly maxQueuedChunks = 4;
    private readonly maxApplyChunksPerCommit = 4;
    private readonly mapUpdateIntervalMs = 80;
    private lastMapUpdateTs = 0;
    private readonly scanUpdateIntervalMs = 120;
    private lastScanUpdateTs = 0;
    private pendingChunks: DecodedCloudChunk[] = [];
    private pendingScanChunk: DecodedCloudChunk | null = null;
    private readonly mapItemName = 'map';
    private readonly scanItemName = 'scan';
    private readonly odomItemName = 'odom';
    realtimeMaxPoints: number = 5_000_000;

    constructor(containerId: string, options: RealtimeUrlOptions = {}) {
        super(containerId);
        this.setRealtimeOptions(options);
        this.setupRealtimeItems();
        this.installRealtimeSection();
    }

    /** Inserts the realtime connection panel above the item dropdown and settings area. */
    private installRealtimeSection(): void {
        if (!this.settingsPanel || !this.settingsContent) return;
        const section = document.createElement('div');
        section.className = 'q3d-settings-section';
        section.setAttribute('data-role', 'realtime');

        section.appendChild(makeLabel('ROS Bridge URL'));
        const rosInput = makeTextInput(this.rosbridgeUrl, v => { this.rosbridgeUrl = v; });
        rosInput.setAttribute('data-role', 'realtime-ros-url');
        this.rosUrlInput = rosInput;

        section.appendChild(rosInput);
        section.appendChild(makeLabel('Max Points / Scan'));
        const maxScanInput = makeNumberInput(this.maxPointsPerScan, 1, 1_000_000, 100, v => { this.maxPointsPerScan = Math.floor(v); });
        maxScanInput.setAttribute('data-role', 'realtime-max-points-per-scan');
        this.maxScanInput = maxScanInput;
        section.appendChild(maxScanInput);

        section.appendChild(makeLabel('Auto Record on Start'));
        const autoRecordInput = document.createElement('input');
        autoRecordInput.type = 'checkbox';
        autoRecordInput.checked = this.autoRecord;
        autoRecordInput.addEventListener('change', () => {
            this.autoRecord = autoRecordInput.checked;
        });
        autoRecordInput.setAttribute('data-role', 'realtime-auto-record');
        this.autoRecordInput = autoRecordInput;
        section.appendChild(autoRecordInput);

        section.appendChild(makeLabel('Max Accumulated Points'));
        const maxCloudInput = makeNumberInput(this.realtimeMaxPoints, 10_000, 50_000_000, 100_000, v => { this.realtimeMaxPoints = Math.floor(v); });
        maxCloudInput.setAttribute('data-role', 'realtime-max-accumulated-points');
        this.maxCloudInput = maxCloudInput;
        section.appendChild(maxCloudInput);

        section.appendChild(makeLabel('System State'));
        const statusPanel = document.createElement('div');
        statusPanel.className = 'q3d-runtime-status';
        statusPanel.setAttribute('data-role', 'realtime-system-state');
        statusPanel.appendChild(this.makeRuntimeStatusRow('slam', 'SLAM'));
        statusPanel.appendChild(this.makeRuntimeStatusRow('livox', 'Livox'));
        statusPanel.appendChild(this.makeRuntimeStatusRow('record', 'Record'));
        statusPanel.appendChild(this.makeRuntimeStatusRow('camera', 'Camera'));
        section.appendChild(statusPanel);

        const connectBtn = makeButton('Connect', () => {
            const url = rosInput.value.trim();
            if (!url) return;
            this.rosbridgeUrl = url;
            this.connectRosbridge(url);
            connectBtn.textContent = 'Reconnect';
        });
        section.appendChild(connectBtn);

        const startBtn = makeButton('Start SLAM', () => {
            this.sendSwitchRequest(true);
        });
        startBtn.setAttribute('data-role', 'realtime-start-slam');
        section.appendChild(startBtn);

        const endBtn = makeButton('End SLAM', () => {
            this.sendSwitchRequest(false);
        });
        endBtn.setAttribute('data-role', 'realtime-end-slam');
        section.appendChild(endBtn);

        const itemSelect = this.settingsItemSelect?.closest('.q3d-material-select') as HTMLElement | null;
        const itemLabel = this.settingsPanel.querySelector('[data-role="settings-item-label"]') as HTMLElement | null;
        const anchor = itemLabel ?? itemSelect;
        if (anchor?.parentElement === this.settingsPanel) {
            if (itemSelect) itemSelect.style.marginTop = '2px';
            this.settingsPanel.insertBefore(section, anchor);
        } else {
            this.settingsPanel.insertBefore(section, this.settingsContent);
        }
        this.syncRealtimeControls();
        this.updateAllRuntimeStatus('unknown');
    }

    private makeRuntimeStatusRow(kind: 'slam' | 'livox' | 'record' | 'camera', label: string): HTMLElement {
        const row = document.createElement('div');
        row.className = 'q3d-slam-status';
        row.setAttribute('data-role', `realtime-${kind}-state`);

        const name = document.createElement('span');
        name.className = 'q3d-slam-status-name md-typescale-body-medium';
        name.textContent = label;

        const led = document.createElement('span');
        led.className = 'q3d-slam-led q3d-slam-led--unknown';
        led.setAttribute('data-role', `realtime-${kind}-led`);

        const text = document.createElement('span');
        text.className = 'q3d-slam-status-text md-typescale-body-medium';
        text.textContent = 'Unknown';
        text.setAttribute('data-role', `realtime-${kind}-status-text`);

        row.appendChild(name);
        row.appendChild(led);
        row.appendChild(text);

        this.statusLedElements[kind] = led;
        this.statusTextElements[kind] = text;
        return row;
    }

    private syncRealtimeControls(): void {
        if (this.rosUrlInput) this.rosUrlInput.value = this.rosbridgeUrl;
        if (this.autoRecordInput) this.autoRecordInput.checked = this.autoRecord;
        if (this.maxScanInput) this.maxScanInput.value = this.maxPointsPerScan.toString();
        if (this.maxCloudInput) this.maxCloudInput.value = this.realtimeMaxPoints.toString();
    }

    override onSettingsItemSelected(name: string): void {
        const item = this.items[name];
        if (item instanceof NativeCloudItem && this.settingsContent) {
            this.settingsContent.innerHTML = '';
            buildNativeCloudItemSettings(
                item,
                this.settingsContent,
                () => this.requestRender(),
                mode => { this.mapColorMode = mode; },
            );
            return;
        }
        super.onSettingsItemSelected(name);
    }

    setRealtimeOptions(options: RealtimeUrlOptions): void {
        if (options.rosbridgeUrl) this.rosbridgeUrl = options.rosbridgeUrl;
        if (typeof options.autoRecord === 'boolean') this.autoRecord = options.autoRecord;
        if (typeof options.maxPointsPerScan === 'number' && options.maxPointsPerScan > 0) {
            this.maxPointsPerScan = Math.floor(options.maxPointsPerScan);
        }
        if (typeof options.maxAccumulatedPoints === 'number' && options.maxAccumulatedPoints > 0) {
            this.realtimeMaxPoints = Math.floor(options.maxAccumulatedPoints);
        }
        this.syncRealtimeControls();
    }

    connectRosbridge(wsUrl: string, options: RealtimeTopicOptions = {}): void {
        this.rosbridgeUrl = wsUrl;
        this.setRealtimeOptions(options);
        this.mapColorMode = null;
        this.pendingChunks = [];
        this.pendingScanChunk = null;
        this.lastMapUpdateTs = 0;
        this.lastScanUpdateTs = 0;
        this.setupRealtimeItems();
        const map = this.items[this.mapItemName];
        if (map instanceof NativeCloudItem) {
            map.reset(this.realtimeMaxPoints);
            map.setColorMode('FLAT');
        }

        if (this.rosClient?.isActive()) {
            this.disconnectRosbridge();
        }

        const client = new RosbridgeClient({ url: wsUrl });
        const cloudTopic = new Topic<PointCloud2Json>(
            client,
            this.cloudTopicName,
            'sensor_msgs/PointCloud2',
            { queueLength: 1, throttleRate: 0 },
        );
        const odomTopic = new Topic<OdomJson>(
            client,
            this.odomTopicName,
            'nav_msgs/Odometry',
            { queueLength: 1, throttleRate: 0 },
        );
        this.rosClient = client;
        this.cloudTopic = cloudTopic;
        this.odomTopic = odomTopic;
        this.statusQueryInFlight = false;

        client.onReady(() => {
            cloudTopic.subscribe((pointCloud2) => {
                if (this.rosClient !== client) return;
                this.ingestPointCloud2(pointCloud2, options);
            });
            odomTopic.subscribe((odom) => {
                if (this.rosClient !== client) return;
                this.ingestOdometry(odom);
            });
            this.updateAllRuntimeStatus('unknown', 'Checking...');
            this.startSlamStatusPolling();
            this.querySlamStatus();
        });

        client.on('close', () => {
            if (this.rosClient === client) {
                this.rosClient = null;
                this.cloudTopic = null;
                this.odomTopic = null;
            }
            this.statusQueryInFlight = false;
            this.stopSlamStatusPolling();
            this.updateAllRuntimeStatus('unknown');
        });

        client.on('error', (err) => {
            console.error('rosbridge websocket error:', err);
        });

        client.run();
    }

    disconnectRosbridge(): void {
        const client = this.rosClient;
        const cloudTopic = this.cloudTopic;
        const odomTopic = this.odomTopic;
        if (!client) return;

        if (client.isOpen()) {
            cloudTopic?.unsubscribe();
            odomTopic?.unsubscribe();
        }
        client.close();
        this.rosClient = null;
        this.cloudTopic = null;
        this.odomTopic = null;
        this.statusQueryInFlight = false;
        this.stopSlamStatusPolling();
        this.updateAllRuntimeStatus('unknown');
    }

    ingestPointCloud2(pointCloud2: PointCloud2Json, options: RealtimeTopicOptions = {}): void {
        if (this.mapColorMode === null) {
            this.mapColorMode = inferColorModeFromFields(pointCloud2.fields);
            const map = this.items[this.mapItemName];
            if (map instanceof NativeCloudItem) {
                map.setColorMode(this.mapColorMode);
            }
        }

        const decoded = decodePointCloud2(
            pointCloud2,
            options.maxPointsPerScan ?? this.maxPointsPerScan,
            this.mapColorMode,
        );
        if (!decoded) return;

        const maxAccumulatedPoints = options.maxAccumulatedPoints ?? this.realtimeMaxPoints;
        const chunk: DecodedCloudChunk = {
            positions: decoded.positions,
            values: decoded.values,
            rgb: decoded.rgb,
            maxAccumulatedPoints,
        };

        if (this.pendingChunks.length >= this.maxQueuedChunks) {
            this.pendingChunks.shift();
        }
        this.pendingChunks.push(chunk);
        this.pendingScanChunk = chunk;

        this.requestRender();
    }

    override render(): void {
        this.applyPendingCloudUpdates();
        super.render();
        const map = this.items[this.mapItemName];
        if (map instanceof NativeCloudItem) {
            map.draw(this.renderer, this.camera);
        }

        if (this.pendingChunks.length > 0 || this.pendingScanChunk) {
            this.requestRender();
        }
    }

    private sendSwitchRequest(switchOn: boolean): void {
        if (!switchOn) {
            this.resetRealtimeCloudItems();
        }

        if (!this.rosClient?.isOpen()) {
            if (this.statusElement) {
                this.statusElement.textContent = 'ROS bridge is not connected. Press Connect first.';
            }
            return;
        }

        this.sendSwitchRequestWithService(switchOn, this.controlServiceName);
    }

    private sendSwitchRequestWithService(switchOn: boolean, serviceName: string): void {
        void this.sendSwitchRequestWithServiceAsync(switchOn, serviceName);
    }

    private async sendSwitchRequestWithServiceAsync(switchOn: boolean, serviceName: string): Promise<void> {
        const client = this.rosClient;
        if (!client?.isOpen()) return;

        const fallbackServiceName = this.getAlternateServiceName(serviceName);
        const action = switchOn ? 'start' : 'end';
        const service = new Service<Record<string, unknown>, SlamSwitchValues>(client, serviceName);

        this.updateRuntimeStatus('slam', 'unknown', switchOn ? 'Starting...' : 'Stopping...');
        if (this.statusElement) {
            this.statusElement.textContent = switchOn
                ? `Starting SLAM via ${serviceName}...`
                : `Ending SLAM via ${serviceName}...`;
        }

        try {
            const values = await service.call(new ServiceRequest({
                switch: switchOn,
                record: this.autoRecord,
            }));
            const ok = values.success === true;

            if (!ok && fallbackServiceName && fallbackServiceName !== serviceName) {
                if (this.statusElement) {
                    this.statusElement.textContent =
                        `SLAM ${action} failed via ${serviceName}. Retrying via ${fallbackServiceName}...`;
                }
                await this.sendSwitchRequestWithServiceAsync(switchOn, fallbackServiceName);
                return;
            }

            if (this.statusElement) {
                this.statusElement.textContent = ok
                    ? `SLAM ${action} request succeeded via ${serviceName}.`
                    : `SLAM ${action} request failed via ${serviceName}.`;
            }
        } catch {
            if (fallbackServiceName && fallbackServiceName !== serviceName) {
                if (this.statusElement) {
                    this.statusElement.textContent =
                        `SLAM ${action} failed via ${serviceName}. Retrying via ${fallbackServiceName}...`;
                }
                await this.sendSwitchRequestWithServiceAsync(switchOn, fallbackServiceName);
                return;
            }

            if (this.statusElement) {
                this.statusElement.textContent = `SLAM ${action} request failed via ${serviceName}.`;
            }
        }

        this.querySlamStatus();
    }

    private getAlternateServiceName(serviceName: string): string | null {
        const trimmed = serviceName.trim();
        if (!trimmed) return null;
        return trimmed.startsWith('/') ? trimmed.slice(1) : `/${trimmed}`;
    }

    private deriveStatusServiceName(): string {
        const control = this.controlServiceName.trim();
        if (!control) return this.statusServiceName;
        if (control.endsWith('/switch')) {
            return `${control.slice(0, -'/switch'.length)}/status`;
        }
        return this.statusServiceName;
    }

    private startSlamStatusPolling(): void {
        this.stopSlamStatusPolling();
        this.statusPollTimer = window.setInterval(() => {
            this.querySlamStatus();
        }, this.statusPollIntervalMs);
    }

    private stopSlamStatusPolling(): void {
        if (this.statusPollTimer !== null) {
            window.clearInterval(this.statusPollTimer);
            this.statusPollTimer = null;
        }
        this.statusQueryInFlight = false;
    }

    private querySlamStatus(): void {
        const client = this.rosClient;
        if (!client?.isOpen()) return;
        if (this.statusQueryInFlight) return;

        this.statusQueryInFlight = true;
        const service = new Service<Record<string, unknown>, SlamStatusValues>(
            client,
            this.deriveStatusServiceName(),
        );

        void service.call(new ServiceRequest({}))
            .then(values => {
                this.updateRuntimeStatus('slam', values.slam === true ? 'running' : 'stopped');
                this.updateRuntimeStatus('livox', values.livox === true ? 'running' : 'stopped');
                this.updateRuntimeStatus('record', values.record === true ? 'running' : 'stopped');
                this.updateRuntimeStatus('camera', values.camera === true ? 'running' : 'stopped');
            })
            .catch(() => {
                this.updateAllRuntimeStatus('unknown');
            })
            .finally(() => {
                this.statusQueryInFlight = false;
            });
    }

    private updateRuntimeStatus(kind: 'slam' | 'livox' | 'record' | 'camera', state: 'running' | 'stopped' | 'unknown', text?: string): void {
        const led = this.statusLedElements[kind];
        const label = this.statusTextElements[kind];
        if (!led || !label) return;

        led.classList.remove('q3d-slam-led--running', 'q3d-slam-led--stopped', 'q3d-slam-led--unknown');
        if (state === 'running') {
            led.classList.add('q3d-slam-led--running');
            label.textContent = text ?? 'Running';
            return;
        }
        if (state === 'stopped') {
            led.classList.add('q3d-slam-led--stopped');
            label.textContent = text ?? 'Stopped';
            return;
        }

        led.classList.add('q3d-slam-led--unknown');
        label.textContent = text ?? 'Unknown';
    }

    private updateAllRuntimeStatus(state: 'running' | 'stopped' | 'unknown', text?: string): void {
        this.updateRuntimeStatus('slam', state, text);
        this.updateRuntimeStatus('livox', state, text);
        this.updateRuntimeStatus('record', state, text);
        this.updateRuntimeStatus('camera', state, text);
    }

    private resetRealtimeCloudItems(): void {
        const map = this.items[this.mapItemName];
        if (map instanceof NativeCloudItem) {
            map.reset(this.realtimeMaxPoints);
        }

        const scan = this.items[this.scanItemName];
        if (scan instanceof CloudItem) {
            scan.replacePoints(new Float32Array(0), new Float32Array(0), undefined);
            scan.replacePoints(new Float32Array(0), new Float32Array(0), undefined);
        }

        this.pendingChunks = [];
        this.pendingScanChunk = null;
        this.requestRender();
    }

    private setupRealtimeItems(): void {
        if (!(this.items[this.mapItemName] instanceof NativeCloudItem)) {
            const map = new NativeCloudItem({
                colorMode: 'FLAT',
                pointSize: 1,
                alpha: 1,
            });
            this.addItem(this.mapItemName, map);
        }

        if (!(this.items[this.scanItemName] instanceof CloudItem)) {
            const scan = new CloudItem(
                new Float32Array(0),
                new Float32Array(0),
                {
                    size: 2,
                    alpha: 1,
                    colorMode: 'FLAT',
                    color: '#ffffff',
                },
            );
            this.addItem(this.scanItemName, scan);
        }

        if (!(this.items[this.odomItemName] instanceof AxisItem)) {
            const odom = new AxisItem({ size: 0.5, width: 5 });
            this.addItem(this.odomItemName, odom);
        }
    }

    private ingestOdometry(msg: OdomJson): void {
        const odom = this.items[this.odomItemName];
        if (!(odom instanceof AxisItem)) return;

        const p = msg?.pose?.pose?.position;
        const q = msg?.pose?.pose?.orientation;
        if (!p || !q) return;

        const t = new THREE.Vector3(p.x ?? 0, p.y ?? 0, p.z ?? 0);
        const quat = new THREE.Quaternion(q.x ?? 0, q.y ?? 0, q.z ?? 0, q.w ?? 1).normalize();
        const matrix = new THREE.Matrix4().compose(t, quat, new THREE.Vector3(1, 1, 1));
        odom.setTransform(matrix);
        this.requestRender();
    }

    private applyPendingCloudUpdates(): void {
        if (this.pendingChunks.length === 0 && !this.pendingScanChunk) return;

        this.setupRealtimeItems();
        const map = this.items[this.mapItemName] as NativeCloudItem | undefined;
        const scan = this.items[this.scanItemName] as CloudItem | undefined;
        if (!(map instanceof NativeCloudItem) || !(scan instanceof CloudItem)) return;

        let lastScanCount = 0;
        const chunkToScan = this.pendingScanChunk;
        const now = performance.now();
        const shouldUpdateScan = !!chunkToScan &&
            (now - this.lastScanUpdateTs >= this.scanUpdateIntervalMs || this.pendingChunks.length === 0);
        if (chunkToScan && shouldUpdateScan) {
            scan.replacePoints(chunkToScan.positions, chunkToScan.values, undefined);
            lastScanCount = chunkToScan.values.length;
            this.pendingScanChunk = null;
            this.lastScanUpdateTs = now;
        }

        let applied = 0;
        const shouldCommitMap = this.pendingChunks.length > 0 &&
            (now - this.lastMapUpdateTs >= this.mapUpdateIntervalMs || this.pendingChunks.length >= this.maxQueuedChunks);

        if (shouldCommitMap) {
            while (applied < this.maxApplyChunksPerCommit && this.pendingChunks.length > 0) {
                const chunk = this.pendingChunks.shift();
                if (!chunk) break;

                map.appendPoints(this.renderer, chunk.positions, chunk.values, chunk.maxAccumulatedPoints);
                applied++;
                lastScanCount = chunk.values.length;
            }
            this.lastMapUpdateTs = now;
        }

        if (applied > 0 && this.statusElement) {
            this.statusElement.textContent =
                `Map: ${map.getPointCount().toLocaleString()} pts | Scan: ${lastScanCount.toLocaleString()} pts`;
        }
    }
}
