# Getting Started with Urban

Welcome. Urban is the fastest way to build agentic apps that run on your own machine. This guide introduces Urban, then walks you through two journeys — consuming an app, and authoring one — across three pathways. Pick whichever fits the way you like to work.

## What is Urban

Urban is a framework for writing agentic orchestration applications with Nano, in TypeScript. It runs on your workstation, with no cloud required.

With Urban you compose coding agents, tools, and human approvals into durable workflows. "Durable" means a workflow survives a crash — after a restart it resumes at the exact step it reached, so no work repeats and no tokens are re-spent.

You can author the same app two ways: write the workflow as TypeScript, or draw it as a model. One engine runs both.

Urban is provider-agnostic, and small enough to start on a Raspberry Pi.

### Urban and Nano Studio

Nano Studio is the RAAD — the Rapid Agentic Application Development environment. It gives you the full integrated experience: scaffold, edit, deploy, and observe your Urban apps in one place. Bring your own IDE if you prefer, or work entirely inside Nano Studio.

## Prerequisites

You only need this setup once.

- Install Node.js. Nano needs Node.js version 22.6 or later.
- Install the CLI. Run `npm i -g @camunda8/cli`.
- Load the Nano plugin. Run `c8ctl load plugin c8ctl-plugin-nano`.
- Start a node. Run `c8ctl nano start`.

A node is a single Nano process. It serves the console, the engine, and the agent endpoint.

## Two journeys

There are two journeys in Urban. In the first, you consume an app that someone else built. In the second, you author a brand-new app of your own.

Each journey offers three pathways — think of them as surfaces: Nano Studio, your own IDE or CLI, and the agent endpoint. Choose the one that suits your tools. Whichever you pick, you land in the same place: a deployed app you can watch run in Studio.

## Journey 1 — Consume an existing app

Your goal: run an app that another author built.

### Pathway A — Nano Studio

1. Open the console.
2. Open Studio.
3. Import the app by reference, or open the shared project.
4. Deploy the app.
5. Observe the instances in the Explorer.

### Pathway B — Your own IDE or CLI

1. Clone the app repository, or install the app package.
2. Change into the app directory.
3. Install the dependencies. Run `npm install`.
4. Run the app. Run `npm run dev`.
5. Open the app link from the command output.

### Pathway C — The agent endpoint

1. Give the agent the address `http://<node>/llms.txt`.
2. The agent reads the index. The index links the app surfaces.
3. The agent links the app in.

## Journey 2 — Author a new app

Your goal: build a new app of your own.

### Pathway A — Nano Studio

Nano Studio gives you a first-class **New Urban App** button. The Urban toolkit arrives through the extensions marketplace, so Studio installs it the first time you need it. That first install needs network access, once.

1. Open the console.
2. Open Studio.
3. Select **New Urban App**.
4. Select the **model-first** card, or the **code-first** card.
5. Select the runtime: **Node**.
6. Create the app. Studio scaffolds the app from the toolkit.
7. Deploy the app.
8. Observe the instances in the Explorer.

Running an app needs [Node](https://nodejs.org) ≥ 22.6 on your machine — the sole JS/TS runtime.

### Pathway B — Your own IDE or CLI

Here you write TypeScript, and Nano does the rest. Nano derives the executable model, the job types, the message correlation, and a generic worker — you write only the steps and the handlers.

1. Scaffold the app. Run `npm create urban-app@latest my-app`.
2. To author code-first, add `--style code`. The default style is model-first.
3. Change into the app directory. Run `cd my-app`.
4. Install the dependencies. Run `npm install`.
5. Write the flow with `@nanobpm/workflow`.
6. Run the app. Run `npm run dev`.
7. Deploy the app to the node. Run `npm run deploy`.
8. Observe the app in Studio.

### Pathway C — The agent endpoint

The agent endpoint hands an external coding agent everything it needs to author an app for you.

1. Give the agent the address `http://<node>/agent`.
2. The agent reads the brief. The brief describes the App schema and the run steps.
3. The agent authors `nano.app.json`. The agent links the app in.
4. Deploy the app.
5. Observe the app in Studio.

## Next steps

- Explore the published schemas at [nanobpm.io/schemas](https://nanobpm.io/schemas/).
- Try the engine live in the [browser demo](https://nanobpm.io/demo/).
- Browse the other guides in the sidebar.
