import { router } from 'expo-router';
import { receivePairingIntent, requestPairingScreen, clearPairingIntent } from '../platform/pairing-intent';
import { useEffect, useRef } from 'react';
import * as Linking from '../platform/linking';
import { parseConnectionLaunchUrl } from '../platform/identity-compatibility';
import { useConnection } from './useConnection';

/**
 * Handles Mitsuro deep links for seamless server connection.
 *
 * Supported URLs:
 *   mitsuro://connect?url=https://device.ts.net:8443&token=mitsuro_remote_...
 *   prior-scheme compatibility links issued by an older server
 *
 * Also handles HTTPS universal links:
 *   https://device.ts.net:8443/#mitsuro-remote-token=mitsuro_remote_...
 */
export function useDeepLink() {
  const { connect } = useConnection();
  const handledRef = useRef<string | null>(null);
  const connectRef = useRef(connect);
  connectRef.current = connect;

  useEffect(() => {
    Linking.getInitialURL().then(url => {
      if (url) handleUrl(url);
    });

    const subscription = Linking.addEventListener('url', ({ url }) => {
      handleUrl(url);
    });

    return () => subscription.remove();
  }, []);

  function handleUrl(url: string) {
    // Dedupe — don't handle the same URL twice
    if (handledRef.current === url) return;
    handledRef.current = url;

    if (receivePairingIntent(url)) { router.replace('/pair'); return; }
    try {
      const initial = new URL(url);
      if ((initial.protocol === 'http:' || initial.protocol === 'https:') && initial.pathname === '/pair') requestPairingScreen();
    } catch { /* Legacy readers handle their own URL formats. */ }
    const launch = parseConnectionLaunchUrl(url);
    if (launch) { clearPairingIntent(); connectRef.current(launch.serverUrl, launch.token); }
  }
}
