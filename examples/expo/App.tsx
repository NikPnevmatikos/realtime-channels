import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  AppState,
  FlatList,
  KeyboardAvoidingView,
  Platform,
  Pressable,
  SafeAreaView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { createClient, type ConnectionStatus, type RealtimeClient } from 'realtime-channels';
import { apiKey, appSyncEvents, cognitoUserPool, lambdaAuthorizer } from 'realtime-channels/appsync-events';
import { RealtimeProvider, useChannel, useConnectionStatus } from 'realtime-channels/react';

type AuthMode = 'cognito' | 'lambda' | 'apiKey';

interface LogEntry {
  id: number;
  at: string;
  kind: 'sys' | 'evt' | 'err';
  text: string;
}

/* ---------- small helpers ---------- */

function base64UrlDecode(input: string): string {
  const b64 = input.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (input.length % 4)) % 4);
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  const bytes: number[] = [];
  let bits = 0;
  let value = 0;
  for (const ch of b64) {
    if (ch === '=') break;
    value = (value << 6) | alphabet.indexOf(ch);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((value >> bits) & 0xff);
    }
  }
  // UTF-8 decode without TextDecoder (not available on every Hermes build).
  let out = '';
  for (let i = 0; i < bytes.length; ) {
    const b0 = bytes[i++] ?? 0;
    if (b0 < 0x80) out += String.fromCharCode(b0);
    else if (b0 < 0xe0) out += String.fromCharCode(((b0 & 0x1f) << 6) | ((bytes[i++] ?? 0) & 0x3f));
    else if (b0 < 0xf0) out += String.fromCharCode(((b0 & 0x0f) << 12) | (((bytes[i++] ?? 0) & 0x3f) << 6) | ((bytes[i++] ?? 0) & 0x3f));
    else {
      const cp = ((b0 & 0x07) << 18) | (((bytes[i++] ?? 0) & 0x3f) << 12) | (((bytes[i++] ?? 0) & 0x3f) << 6) | ((bytes[i++] ?? 0) & 0x3f);
      out += String.fromCodePoint(cp);
    }
  }
  return out;
}

function decodeJwt(token: string): { sub?: string; exp?: number } | null {
  try {
    const payload = token.split('.')[1];
    return payload ? (JSON.parse(base64UrlDecode(payload)) as { sub?: string; exp?: number }) : null;
  } catch {
    return null;
  }
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, (_k, v: unknown) => (v instanceof Error ? v.message : v));
  } catch {
    return String(value);
  }
}

function sentAtOf(event: unknown): string | undefined {
  if (event && typeof event === 'object') {
    const e = event as { sentAtUtc?: unknown; sentAt?: unknown };
    if (typeof e.sentAtUtc === 'string') return e.sentAtUtc;
    if (typeof e.sentAt === 'string') return e.sentAt;
  }
  return undefined;
}

/* ---------- screen ---------- */

export default function App() {
  const [httpDomain, setHttpDomain] = useState('');
  const [mode, setMode] = useState<AuthMode>('cognito');
  const [token, setToken] = useState('');
  const [channel, setChannel] = useState('');
  const [client, setClient] = useState<RealtimeClient | null>(null);
  const [extraChannel, setExtraChannel] = useState<string | null>(null);
  const [log, setLog] = useState<LogEntry[]>([]);
  const nextId = useRef(1);

  const append = useCallback((kind: LogEntry['kind'], text: string) => {
    setLog((prev) =>
      [{ id: nextId.current++, at: new Date().toISOString().slice(11, 23), kind, text }, ...prev].slice(0, 200),
    );
  }, []);

  const claims = useMemo(() => decodeJwt(token.trim()), [token]);
  useEffect(() => {
    if (mode === 'cognito' && claims?.sub && channel === '') setChannel(`users/${claims.sub}`);
  }, [claims, mode, channel]);

  // The auth helpers call this on every connect/subscribe, so a pasted-in fresh token is picked up.
  const tokenRef = useRef(token);
  tokenRef.current = token;

  const connect = () => {
    if (!httpDomain.trim() || !channel.trim() || !token.trim()) {
      append('err', 'fill in the HTTP domain, the token and a channel first');
      return;
    }
    client?.close();
    const getToken = () => tokenRef.current.trim();
    const auth =
      mode === 'cognito' ? cognitoUserPool(getToken) : mode === 'lambda' ? lambdaAuthorizer(getToken) : apiKey(getToken());
    const next = createClient(appSyncEvents({ httpDomain: httpDomain.trim(), auth }), {
      logger: (level, message, data) => {
        if (level !== 'debug') append('sys', `[${level}] ${message}${data ? ' ' + safeJson(data) : ''}`);
      },
    });
    next.onStatus((s, info) => append('sys', `status → ${s}${info ? ' ' + safeJson(info) : ''}`));
    next.onError((e) => append('err', `${e.code}: ${e.message}`));
    setExtraChannel(null);
    setClient(next);
  };

  // Reconnect when the app returns to the foreground; the OS drops sockets in the background.
  useEffect(() => {
    if (!client) return;
    const sub = AppState.addEventListener('change', (state) => {
      if (state === 'active' && client.status === 'closed') {
        append('sys', 'app active again → reconnecting');
        client.connect().catch(() => {});
      }
    });
    return () => sub.remove();
  }, [client, append]);

  const expiresIn = claims?.exp ? Math.round((claims.exp * 1000 - Date.now()) / 60000) : null;

  return (
    <SafeAreaView style={styles.safe}>
      <StatusBar style="auto" />
      <KeyboardAvoidingView behavior={Platform.OS === 'ios' ? 'padding' : undefined} style={styles.flex}>
        <Text style={styles.h1}>realtime-channels · AppSync Events</Text>
        <Text style={styles.sub}>Subscribe-only example. Publish from your backend to see events here.</Text>

        <Field label="HTTP domain">
          <TextInput
            style={styles.input}
            autoCapitalize="none"
            autoCorrect={false}
            placeholder="your-api-id.appsync-api.<region>.amazonaws.com"
            value={httpDomain}
            onChangeText={setHttpDomain}
          />
        </Field>

        <Field label="Auth mode">
          <View style={styles.row}>
            {(['cognito', 'lambda', 'apiKey'] as AuthMode[]).map((m) => (
              <Pressable key={m} onPress={() => setMode(m)} style={[styles.chip, mode === m && styles.chipOn]}>
                <Text style={[styles.chipText, mode === m && styles.chipTextOn]}>{m}</Text>
              </Pressable>
            ))}
          </View>
        </Field>

        <Field label={`Token / key${expiresIn !== null ? `  (expires in ${expiresIn} min)` : ''}`}>
          <TextInput
            style={[styles.input, styles.multiline]}
            multiline
            autoCapitalize="none"
            autoCorrect={false}
            placeholder="Paste the Cognito ID token, custom token or API key"
            value={token}
            onChangeText={setToken}
          />
        </Field>

        <Field label="Channel">
          <TextInput
            style={styles.input}
            autoCapitalize="none"
            autoCorrect={false}
            placeholder="users/<sub>"
            value={channel}
            onChangeText={setChannel}
          />
        </Field>

        <View style={styles.row}>
          <Button title="Connect & subscribe" onPress={connect} primary />
          <Button title="Subscribe users/aaa" onPress={() => setExtraChannel('users/aaa')} disabled={!client} />
          <Button title="Close" onPress={() => client?.close()} disabled={!client} />
        </View>

        {client ? (
          <RealtimeProvider client={client}>
            <Connected channel={channel.trim()} extraChannel={extraChannel} append={append} />
          </RealtimeProvider>
        ) : (
          <Text style={styles.status}>idle</Text>
        )}

        <FlatList
          style={styles.log}
          data={log}
          keyExtractor={(item) => String(item.id)}
          renderItem={({ item }) => (
            <Text style={[styles.line, item.kind === 'evt' && styles.evt, item.kind === 'err' && styles.err]}>
              {item.at}  {item.text}
            </Text>
          )}
        />
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

/** Uses the hooks; mounts only once a client exists. */
function Connected({
  channel,
  extraChannel,
  append,
}: {
  channel: string;
  extraChannel: string | null;
  append: (kind: LogEntry['kind'], text: string) => void;
}) {
  const status: ConnectionStatus = useConnectionStatus();

  useChannel(
    channel,
    (event) => {
      let latency = '';
      const sent = sentAtOf(event);
      if (sent) {
        const ms = Date.now() - Date.parse(sent);
        if (!Number.isNaN(ms)) latency = `  (latency ${ms} ms)`;
      }
      append('evt', `event on ${channel}: ${safeJson(event)}${latency}`);
    },
    { onError: (e) => append('err', `subscription error on ${channel}: ${e.code} ${e.message}`) },
  );

  useChannel(extraChannel, (event) => append('evt', `event on ${extraChannel}: ${safeJson(event)}`), {
    onError: (e) => append('err', `subscribe to ${extraChannel} rejected: ${e.code}`),
  });

  return (
    <Text
      style={[
        styles.status,
        status === 'open' && styles.ok,
        (status === 'connecting' || status === 'reconnecting') && styles.warn,
      ]}
    >
      {status}
    </Text>
  );
}

/* ---------- tiny UI bits ---------- */

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <View style={styles.field}>
      <Text style={styles.label}>{label}</Text>
      {children}
    </View>
  );
}

function Button({
  title,
  onPress,
  primary,
  disabled,
}: {
  title: string;
  onPress: () => void;
  primary?: boolean;
  disabled?: boolean;
}) {
  return (
    <Pressable
      onPress={onPress}
      disabled={disabled}
      style={[styles.btn, primary && styles.btnPrimary, disabled && styles.btnDisabled]}
    >
      <Text style={[styles.btnText, primary && styles.btnTextPrimary]}>{title}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#f4f6f8' },
  flex: { flex: 1, padding: 16, gap: 10 },
  h1: { fontSize: 18, fontWeight: '700', color: '#16212b' },
  sub: { fontSize: 13, color: '#66757f', marginBottom: 6 },
  field: { gap: 4 },
  label: { fontSize: 12, color: '#66757f' },
  input: {
    borderWidth: 1,
    borderColor: '#d9e0e6',
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 8,
    backgroundColor: '#fff',
    fontSize: 13,
  },
  multiline: { minHeight: 64, textAlignVertical: 'top' },
  row: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, alignItems: 'center' },
  chip: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 999, borderWidth: 1, borderColor: '#b9c4cd' },
  chipOn: { backgroundColor: '#0b8a5a', borderColor: '#0b8a5a' },
  chipText: { fontSize: 12, color: '#3d4b58' },
  chipTextOn: { color: '#fff' },
  btn: { paddingHorizontal: 12, paddingVertical: 8, borderRadius: 6, borderWidth: 1, borderColor: '#b9c4cd', backgroundColor: '#fff' },
  btnPrimary: { backgroundColor: '#0b8a5a', borderColor: '#0b8a5a' },
  btnDisabled: { opacity: 0.5 },
  btnText: { fontSize: 13, color: '#16212b' },
  btnTextPrimary: { color: '#fff', fontWeight: '600' },
  status: {
    alignSelf: 'flex-start',
    fontSize: 12,
    paddingHorizontal: 10,
    paddingVertical: 3,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: '#b4372f',
    color: '#b4372f',
  },
  ok: { borderColor: '#0b8a5a', color: '#0b8a5a' },
  warn: { borderColor: '#b7791f', color: '#b7791f' },
  log: { flex: 1, borderWidth: 1, borderColor: '#d9e0e6', borderRadius: 6, backgroundColor: '#fff', padding: 8 },
  line: { fontFamily: Platform.OS === 'ios' ? 'Menlo' : 'monospace', fontSize: 11.5, color: '#3d4b58', marginBottom: 4 },
  evt: { color: '#0b8a5a' },
  err: { color: '#b4372f' },
});
